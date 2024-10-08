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

import com.timeplus.jdbc.TimeplusArray;
import com.timeplus.data.type.complex.DataTypeArray;
import com.timeplus.serde.BinarySerializer;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ColumnArray extends AbstractColumn {

    private final List<Long> offsets;
    // data represents nested column in ColumnArray
    private final IColumn nestedColumn;

    public ColumnArray(String name, DataTypeArray type, Object[] values) {
        super(name, type, values);
        offsets = new ArrayList<>();
        nestedColumn = ColumnFactory.createColumn(null, type.getElemDataType(), null);
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        Object[] arr = ((TimeplusArray) object).getArray();

        offsets.add(offsets.isEmpty() ? arr.length : offsets.get((offsets.size() - 1)) + arr.length);
        for (Object field : arr) {
            nestedColumn.write(field);
        }
    }

    public void flushOffsets(BinarySerializer serializer) throws IOException {
        for (long offsetList : offsets) {
            serializer.writeLong(offsetList);
        }
    }

    @Override
    public void setColumnWriterBuffer(ColumnWriterBuffer buffer) {
        super.setColumnWriterBuffer(buffer);
        nestedColumn.setColumnWriterBuffer(buffer);
    }

    @Override
    public void clear() {
        offsets.clear();
        nestedColumn.clear();
    }

    @Override
    public void SerializeBulkPrefix(BinarySerializer serializer) throws SQLException, IOException {
        nestedColumn.SerializeBulkPrefix(serializer);
    }

    @Override
    public void SerializeBulk(BinarySerializer serializer, Boolean now) throws IOException, SQLException {
        flushOffsets(serializer);
        nestedColumn.SerializeBulk(serializer, false);

        if (now) {
            buffer.writeTo(serializer);
        }
    }

    @Override
    public void SerializeBulkSuffix(BinarySerializer serializer) throws SQLException, IOException {
        nestedColumn.SerializeBulkSuffix(serializer);
    }

 
}
