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

import com.timeplus.client.NativeContext;
import com.timeplus.data.IDataType;
import com.timeplus.exception.InvalidOperationException;
import com.timeplus.misc.SQLLexer;
import com.timeplus.serde.BinaryDeserializer;
import com.timeplus.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;

public class DataTypeNothing implements IDataType<Object, Object> {

    public static DataTypeCreator<Object, Object> CREATOR =
            (lexer, serverContext) -> new DataTypeNothing(serverContext);

    public DataTypeNothing(NativeContext.ServerContext serverContext) {
    }

    @Override
    public String name() {
        return "nothing";
    }

    @Override
    public int sqlTypeId() {
        return Types.NULL;
    }

    @Override
    public Object defaultValue() {
        return null;
    }

    @Override
    public Class<Object> javaType() {
        return Object.class;
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
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeByte((byte) 0);
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        deserializer.readByte();
        return null;
    }

    @Override
    public String[] getAliases() {
        return new String[]{"null"};
    }

    @Override
    public Object deserializeText(SQLLexer lexer) throws SQLException {
        throw new InvalidOperationException("Nothing datatype can't deserializeText");
    }

}
