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

import com.timeplus.data.DataTypeFactory;
import com.timeplus.data.IDataType;
import com.timeplus.data.IndexType;
import com.timeplus.data.type.DataTypeUInt8;
import com.timeplus.data.type.DataTypeUInt16;
import com.timeplus.data.type.DataTypeUInt32;
import com.timeplus.data.type.DataTypeUInt64;
import com.timeplus.misc.SQLLexer;
import com.timeplus.misc.Validate;
import com.timeplus.serde.BinaryDeserializer;
import com.timeplus.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeLowCardinality implements IDataType<Object, Object>  {

    public static DataTypeCreator<Object, Object>  creator = (lexer, serverContext) -> {
        Validate.isTrue(lexer.character() == '(');
        IDataType<?, ?>  nestedType = DataTypeFactory.get(lexer, serverContext);
        Validate.isTrue(lexer.character() == ')');
        return new DataTypeLowCardinality(
                "low_cardinality(" + nestedType.name() + ")", nestedType);
    };

    private final String name;
    private final IDataType<?, ?>  nestedDataType;
    private final Long version = 1L;
    private final Long IndexTypeMask = 0b11111111L;
    private boolean nested_is_nullable;

    public DataTypeLowCardinality(String name, IDataType<?, ?>  nestedDataType) {
        this.name = name;
        this.nestedDataType = nestedDataType;
        if (nestedDataType.nullable()) {
            nested_is_nullable = true;
        }
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public int sqlTypeId() {
        return this.nestedDataType.sqlTypeId();
    }

    @Override
    public Object defaultValue() {
        return this.nestedDataType.defaultValue();
    }

    @Override
    public Class javaType() {
        return this.nestedDataType.javaType();
    }

    @Override
    public Class jdbcJavaType() {
        return this.nestedDataType.jdbcJavaType();
    }

    @Override
    public boolean nullable() {
        return this.nestedDataType.nullable();
    }

    @Override
    public int getPrecision() {
        return this.nestedDataType.getPrecision();
    }

    @Override
    public int getScale() {
        return this.nestedDataType.getScale();
    }

    @Override
    public Object deserializeText(SQLLexer lexer) throws SQLException {
        return this.nestedDataType.deserializeText(lexer);
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        getNestedTypes().serializeBinary(data, serializer);
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        getNestedTypes().serializeBinaryBulk(data, serializer);
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return this.getNestedTypes().deserializeBinary(deserializer);
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        if (rows == 0) {
            Object[] data = getNestedTypes().deserializeBinaryBulk(rows, deserializer);
            return data;
        }
        else {
            Long index_type = deserializer.readLong() & IndexTypeMask;
            Long key_nums = deserializer.readLong();
            Object[] dictionary = new Object[key_nums.intValue()];
            IDataType inner_type;

            if (nested_is_nullable) {
                DataTypeNullable type = (DataTypeNullable) getNestedTypes();
                inner_type = type.getNestedDataType();
            }
            else {
                inner_type = getNestedTypes();
            }
            dictionary = inner_type.deserializeBinaryBulk(key_nums.intValue(), deserializer);
            dictionary[0] = null;
            Long row_nums = deserializer.readLong();

            if (row_nums != rows) {
                throw new SQLException("read unexpected rows in low_cardinality, expected:" + rows + ", actual:" + row_nums);
            }

            IDataType type;

            if (index_type == IndexType.UInt8.getValue()) {
                type = new DataTypeUInt8();
            }
            else if (index_type == IndexType.UInt16.getValue()) {
                type = new DataTypeUInt16();
            }
            else if (index_type == IndexType.UInt32.getValue()) {
                type = new DataTypeUInt32();
            }
            else {
                type = new DataTypeUInt64();
            }

            Object[] index_data = type.deserializeBinaryBulk(rows, deserializer);
            Object[] data = new Object[rows];

            if (type instanceof DataTypeUInt8) {
                for (int i = 0; i < rows; i++) {
                    data[i] = dictionary[(short) index_data[i]];
                }
            }
            else {
                for (int i = 0; i < rows; i++) {
                    data[i] = dictionary[(Integer) index_data[i]];
                }
            }
            return data;
        }   
    }

    @Override
    public boolean isSigned() {
        return this.nestedDataType.isSigned();
    }

    public IDataType getNestedTypes() {
        return nestedDataType;
    }

    @Override
    public void deserializeBinaryPrefix(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        if (rows != 0) {
            Long version = deserializer.readLong();

            if (version != this.version) {
                throw new SQLException("version error in type low_cardinality");
            }
        }
    }
    
    @Override
    public void deserializeBinarySuffix(int rows, BinaryDeserializer deserializer) throws SQLException, IOException { }

}
