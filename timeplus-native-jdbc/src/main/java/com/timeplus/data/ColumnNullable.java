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

import com.timeplus.data.type.complex.DataTypeNullable;
import com.timeplus.serde.BinarySerializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ColumnNullable extends AbstractColumn {

    private final List<Byte> nullableSign;
    // data represents nested column in ColumnArray
    private final IColumn nestedColumn;

    public ColumnNullable(String name, DataTypeNullable type, Object[] values) {
        super(name, type, values);
        nullableSign = new ArrayList<>();
        nestedColumn = ColumnFactory.createColumn(null, type.getNestedDataType(), null);
    }

    @Override
    public void write(@Nullable Object object) throws IOException, SQLException {
        if (object == null) {
            nullableSign.add((byte) 1);
            nestedColumn.write(type.defaultValue()); // write whatever for padding
        } else {
            nullableSign.add((byte) 0);
            nestedColumn.write(object);
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);
        nestedColumn.setColumnWriterBuffer(buffer);
    }


    @Override
    public void SerializeBulk(BinarySerializer serializer, Boolean now) throws IOException, SQLException {
        for (byte sign : nullableSign) {
            serializer.writeByte(sign);
        }

        if (now)
            buffer.writeTo(serializer);
    }

}
