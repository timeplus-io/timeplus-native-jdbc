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

package com.timeplus.data.type;

import com.timeplus.misc.SQLLexer;
import com.timeplus.serde.BinaryDeserializer;
import com.timeplus.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataTypeInt16 implements BaseDataTypeInt16<Short, Short> {

    @Override
    public String name() {
        return "int16";
    }

    @Override
    public Short defaultValue() {
        return 0;
    }

    @Override
    public Class<Short> javaType() {
        return Short.class;
    }

    @Override
    public int getPrecision() {
        return 6;
    }


    @Override
    public void serializeBinary(Short data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeShort(data);
    }

    @Override
    public Short deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readShort();
    }

    @Override
    public String[] getAliases() {
        return new String[]{"SMALLINT"};
    }

    @Override
    public Short deserializeText(SQLLexer lexer) throws SQLException {
        return lexer.numberLiteral().shortValue();
    }

    @Override
    public boolean isSigned() {
        return true;
    }

}
