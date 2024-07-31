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

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.timeplus.data.type.complex.DataTypeMap;
import com.timeplus.serde.BinarySerializer;

/**
 * @author liuxinghua02
 */
public class ColumnMap extends AbstractColumn {
    private final IColumn[] columnDataArray;

    private final List<Long> offsets;

    public ColumnMap(String name, DataTypeMap type, Object[] values) {
        super(name, type, values);
        offsets = new ArrayList<>();
        IDataType<?, ?>[] types = type.getNestedTypes();
        columnDataArray = new IColumn[types.length];
        for (int i = 0; i < types.length; i++) {
            columnDataArray[i] = ColumnFactory.createColumn(null, types[i], null);
        }
    }

    @Override
    public void write(Object object) throws IOException, SQLException {
        if (object instanceof Map) {
            Map<?, ?> dataMap = (Map<?, ?>) object;
            offsets.add(offsets.isEmpty() ? dataMap.size() : offsets.get((offsets.size() - 1)) + dataMap.size());

            for (Object key : dataMap.keySet()) {
                columnDataArray[0].write(key);
            }
            for (Object value : dataMap.values()) {
                columnDataArray[1].write(value);
            }
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

        for (IColumn nestedColumn : columnDataArray) {
            nestedColumn.setColumnWriterBuffer(new ColumnWriterBuffer());
        }
    }

    @Override
    public void clear() {
        offsets.clear();
        for (IColumn nestedColumn : columnDataArray) {
            nestedColumn.clear();
        }
    }
    
    @Override
    public void SerializeBulkPrefix(BinarySerializer serializer) throws SQLException, IOException {
        for (IColumn nestedColumn : columnDataArray) {
            nestedColumn.SerializeBulkPrefix(serializer);
        }
    }

    @Override
    public void SerializeBulk(BinarySerializer serializer, Boolean now) throws IOException, SQLException {

        flushOffsets(serializer);

        for (IColumn nestedColumn : columnDataArray) {
            nestedColumn.SerializeBulk(serializer, true);
        }

        if (now) {
            buffer.writeTo(serializer);
        }
    }

}
