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

import com.timeplus.data.IDataType;
import com.timeplus.misc.SQLLexer;
import com.timeplus.serde.BinaryDeserializer;
import com.timeplus.serde.BinarySerializer;
 
import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Types;
 
public class DataTypeInt256 implements IDataType<BigInteger, BigInteger> {

    @Override
    public String name() {
        return "int256";
    }

    @Override
    public int sqlTypeId() {
        return Types.BIGINT;
    }

    @Override
    public BigInteger defaultValue() {
        return BigInteger.ZERO;
    }

    @Override
    public Class<BigInteger> javaType() {
        return BigInteger.class;
    }

    @Override
    public int getPrecision() {
        return 78;  //should be 77 but add one
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public void serializeBinary(BigInteger data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeBigInteger(data, 32);
    }

    @Override
    public BigInteger deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        byte[] bytes = deserializer.readBytes(32);
        return new BigInteger(deserializer.reversesBytes(bytes));  // force it to be positive
    }

    @Override
    public BigInteger deserializeText(SQLLexer lexer) throws SQLException {
        String Integer256 = lexer.stringLiteral();
        return new BigInteger(Integer256, 10);
    }

}
