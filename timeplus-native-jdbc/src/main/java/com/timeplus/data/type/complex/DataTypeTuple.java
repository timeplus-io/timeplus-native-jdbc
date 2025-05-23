/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.timeplus.data.type.complex;

import com.timeplus.jdbc.TimeplusStruct;
import com.timeplus.data.DataTypeFactory;
import com.timeplus.data.IDataType;
import com.timeplus.misc.SQLLexer;
import com.timeplus.misc.Validate;
import com.timeplus.serde.BinaryDeserializer;
import com.timeplus.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class DataTypeTuple implements IDataType<TimeplusStruct, Struct> {

    public static DataTypeCreator<TimeplusStruct, Struct> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        List<IDataType<?, ?>> nestedDataTypes = new ArrayList<>();

        Integer count = 0;
        List<String> tupleDataNames = new ArrayList<>();
        for (; ; ) {
            // Tuple return types can be specified as either "name + type" or just "type."
            // Example: tuple(a string, b string) or tuple(string, string)
            // We use the parameter count to determine the possible return types.
            // If the first word is not a type, it's treated as a tuple name.
            // The next word must be a data type; otherwise, an error is reported.
            while (count < 2) {
                String elemName = String.valueOf(lexer.bareWordView());
                try {
                    nestedDataTypes.add(DataTypeFactory.get(lexer, serverContext));
                    count = 0;
                    break;
                } catch (Exception e) {
                    count++;
                    tupleDataNames.add(elemName);
                    if (count >= 2) {
                        throw e;
                    }
                }
            }
            char delimiter = lexer.character();
            Validate.isTrue(delimiter == ',' || delimiter == ')');
            if (delimiter == ')') {
                StringBuilder builder = new StringBuilder("tuple(");
                for (int i = 0; i < nestedDataTypes.size(); i++) {
                    if (i > 0)
                        builder.append(",");
                    if (i < tupleDataNames.size()) {
                        builder.append(tupleDataNames.get(i));
                        builder.append(" ");
                    }
                    builder.append(nestedDataTypes.get(i).name());
                }
                return new DataTypeTuple(builder.append(")").toString(), nestedDataTypes.toArray(new IDataType[0]));
            }
        }
    };

    private final String name;
    private final IDataType<?, ?>[] nestedTypes;

    public DataTypeTuple(String name, IDataType<?, ?>[] nestedTypes) {
        this.name = name;
        this.nestedTypes = nestedTypes;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.STRUCT;
    }

    @Override
    public TimeplusStruct defaultValue() {
        Object[] attrs = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            attrs[i] = getNestedTypes()[i].defaultValue();
        }
        return new TimeplusStruct("tuple", attrs);
    }

    @Override
    public Class<TimeplusStruct> javaType() {
        return TimeplusStruct.class;
    }

    @Override
    public Class<Struct> jdbcJavaType() {
        return Struct.class;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public void serializeBinary(TimeplusStruct data, BinarySerializer serializer) throws SQLException, IOException {
        for (int i = 0; i < getNestedTypes().length; i++) {
            getNestedTypes()[i].serializeBinary(data.getAttributes()[i], serializer);
        }
    }

    @Override
    public void serializeBinaryBulk(TimeplusStruct[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (int i = 0; i < getNestedTypes().length; i++) {
            Object[] elemsData = new Object[data.length];
            for (int row = 0; row < data.length; row++) {
                elemsData[row] = ((Struct) data[row]).getAttributes()[i];
            }
            getNestedTypes()[i].serializeBinaryBulk(elemsData, serializer);
        }
    }

    @Override
    public TimeplusStruct deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] attrs = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            attrs[i] = getNestedTypes()[i].deserializeBinary(deserializer);
        }
        return new TimeplusStruct("tuple", attrs);
    }

    @Override
    public TimeplusStruct[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[][] rowsWithElems = getRowsWithElems(rows, deserializer);

        TimeplusStruct[] rowsData = new TimeplusStruct[rows];
        for (int row = 0; row < rows; row++) {
            Object[] elemsData = new Object[getNestedTypes().length];

            for (int elemIndex = 0; elemIndex < getNestedTypes().length; elemIndex++) {
                elemsData[elemIndex] = rowsWithElems[elemIndex][row];
            }
            rowsData[row] = new TimeplusStruct("tuple", elemsData);
        }
        return rowsData;
    }

    private Object[][] getRowsWithElems(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        Object[][] rowsWithElems = new Object[getNestedTypes().length][];
        for (int index = 0; index < getNestedTypes().length; index++) {
            rowsWithElems[index] = getNestedTypes()[index].deserializeBinaryBulk(rows, deserializer);
        }
        return rowsWithElems;
    }

    @Override
    public TimeplusStruct deserializeText(SQLLexer lexer) throws SQLException {
        Validate.isTrue(lexer.character() == '(');
        Object[] tupleData = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            if (i > 0)
                Validate.isTrue(lexer.character() == ',');
            tupleData[i] = getNestedTypes()[i].deserializeText(lexer);
        }
        Validate.isTrue(lexer.character() == ')');
        return new TimeplusStruct("tuple", tupleData);
    }

    public IDataType[] getNestedTypes() {
        return nestedTypes;
    }

    @Override
    public void deserializeBinaryPrefix(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        for (int i = 0; i < getNestedTypes().length; i++) {
            getNestedTypes()[i].deserializeBinaryPrefix(rows, deserializer);
        }
    }
    
    @Override
    public void deserializeBinarySuffix(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        for (int i = 0; i < getNestedTypes().length; i++) {
            getNestedTypes()[i].deserializeBinarySuffix(rows, deserializer);
        }
    }
}
