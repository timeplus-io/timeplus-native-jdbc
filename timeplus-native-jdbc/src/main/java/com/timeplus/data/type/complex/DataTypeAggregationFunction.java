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
import com.timeplus.data.AggregationType;
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

public class DataTypeAggregationFunction implements IDataType<TimeplusStruct, Struct> {

    public static DataTypeCreator<TimeplusStruct, Struct> creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        List<IDataType<?, ?>> nestedDataTypes = new ArrayList<>();

        String functionTypeName = String.valueOf(lexer.bareWord());
        AggregationType.isValidType(functionTypeName);
        char delimiter = lexer.character();
        Validate.isTrue(delimiter == ',');

        for (; ; ) {
            nestedDataTypes.add(DataTypeFactory.get(lexer, serverContext));
            delimiter = lexer.character();
            Validate.isTrue(delimiter == ',' || delimiter == ')');
            if (delimiter == ')') {
                StringBuilder builder = new StringBuilder("aggregate_function(");
                for (int i = 0; i < nestedDataTypes.size(); i++) {
                    if (i > 0) {
                        builder.append(",");
                    }
                    else {
                        builder.append(functionTypeName);
                        builder.append(",");
                    }
                    builder.append(nestedDataTypes.get(i).name());
                }
                return new DataTypeAggregationFunction(builder.append(")").toString(), nestedDataTypes.toArray(new IDataType[0]));
            }
        }
    };

    private final String name;
    private final IDataType<?, ?>[] nestedTypes;
    private final List<Object> data;

    public DataTypeAggregationFunction(String name, IDataType<?, ?>[] nestedTypes) {
        this.name = name;
        this.nestedTypes = nestedTypes;
        data = new ArrayList<>();
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
        return new TimeplusStruct("aggregate_function", attrs);
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
    }

    @Override
    public void serializeBinaryBulk(TimeplusStruct[] data, BinarySerializer serializer) throws SQLException, IOException {
        
    }

    @Override
    public TimeplusStruct deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Object[] attrs = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            attrs[i] = getNestedTypes()[i].deserializeBinary(deserializer);
        }
        return new TimeplusStruct("aggregate_function", attrs);
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        if (rows == 0) {
            TimeplusStruct[] rowsData = new TimeplusStruct[rows];
            return rowsData;
        }
        else {
            while (deserializer.remaining()) {
                Object l = deserializer.readByte();
                data.add(l);
            }
            return data.toArray();
        }
    }

    @Override
    public TimeplusStruct deserializeText(SQLLexer lexer) throws SQLException {
        Validate.isTrue(lexer.character() == '(');
        Object[] Data = new Object[getNestedTypes().length];
        for (int i = 0; i < getNestedTypes().length; i++) {
            if (i > 0)
                Validate.isTrue(lexer.character() == ',');
                Data[i] = getNestedTypes()[i].deserializeText(lexer);
        }
        Validate.isTrue(lexer.character() == ')');
        return new TimeplusStruct("aggregate_function", Data);
    }

    public IDataType[] getNestedTypes() {
        return nestedTypes;
    }

}
