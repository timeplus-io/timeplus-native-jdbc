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

import com.timeplus.jdbc.TimeplusStruct;
import com.timeplus.data.type.complex.DataTypeTuple;
import com.timeplus.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class ColumnTuple extends AbstractColumn {

    // data represents nested column in ColumnArray
    private final IColumn[] columnDataArray;

    public ColumnTuple(String name, DataTypeTuple type, Object[] values) {
        super(name, type, values);

        IDataType<?, ?>[] types = type.getNestedTypes();
        columnDataArray = new IColumn[types.length];
        for (int i = 0; i < types.length; i++) {
            columnDataArray[i] = ColumnFactory.createColumn(null, types[i], null);
        }
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        TimeplusStruct tuple = (TimeplusStruct) object;
        for (int i = 0; i < columnDataArray.length; i++) {
            columnDataArray[i].write(tuple.getAttributes()[i]);
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);

        for (IColumn nestedColumn : columnDataArray) {
            nestedColumn.setColumnWriterBuffer(new ColumnWriterBuffer());
        }
    }

    @Override
    public void clear() {
    }

    @Override
    public void SerializeBulkPrefix(BinarySerializer serializer) throws SQLException, IOException {
        for (IColumn nestedColumn : columnDataArray) {
            nestedColumn.SerializeBulkPrefix(serializer);
        }
    }

    @Override
    public void SerializeBulk(BinarySerializer serializer, Boolean now) throws IOException, SQLException {
        // we should to flush all the nested data to serializer
        // because they are using separate buffers.
        for (IColumn nestedColumn : columnDataArray) {
            nestedColumn.SerializeBulk(serializer, true);
        }

        if (now) {
            buffer.writeTo(serializer);
        }
    }

}
