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

package com.timeplus.data;

import com.timeplus.data.type.complex.DataTypeLowCardinality;
import com.timeplus.data.type.complex.DataTypeNullable;
import com.timeplus.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ColumnLowCardinality extends AbstractColumn {

    private final List<Long> indexes;
    private final List<Object> dict;
    private final Long version = 1L;
    private boolean nested_is_nullable;
    private IDataType nested_type;

    public ColumnLowCardinality(String name, DataTypeLowCardinality type, Object[] values) {
        super(name, type, values);
        indexes = new ArrayList<>();
        dict = new ArrayList<>();
        nested_is_nullable = type.getNestedTypes().nullable();
        /// If a nested type is nullable, always add two hard dictionary keys in front: [0]: null, [1]: default value
        if (nested_is_nullable) {
            nested_type = ((DataTypeNullable) type.getNestedTypes()).getNestedDataType();
            dict.add(type.getNestedTypes().defaultValue());
            dict.add(type.getNestedTypes().defaultValue());
        }
        else {
            nested_type = type.getNestedTypes();
        }
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        if (object == null) {
            if (nested_is_nullable) {
                indexes.add(0l);
            }
            else {
                throw new SQLException("null object appeared without nullable field");
            }
        }
        else {
            long value = dict.lastIndexOf(object);
            if (value != -1) { 
                indexes.add(value);
            }
            else {
                indexes.add((long) dict.size());
                dict.add(object);
            }
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);
    }

    @Override
    public void clear() {
    }

    @Override
    public void SerializeBulkPrefix(BinarySerializer serializer)  throws IOException, SQLException {
        serializer.writeLong(version);
    }

    @Override
    public void SerializeBulk(BinarySerializer serializer, Boolean now) throws IOException, SQLException {
        /// The data layout: [index_type][dictionary][indexes]
        serializer.writeLong(IndexType.UInt64.getValue() | IndexType.HasAdditionalKeysBit.getValue());
        serializer.writeLong(dict.size());

        nested_type.serializeBinaryBulk(dict.toArray(), serializer);

        serializer.writeLong(indexes.size()); //  give index type size
        for (int i = 0; i < indexes.size(); i++) {
            serializer.writeLong(indexes.get(i));
        }
    
    }

}
