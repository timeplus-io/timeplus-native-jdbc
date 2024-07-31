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

import com.timeplus.misc.BytesHelper;
import com.timeplus.misc.SQLLexer;
import com.timeplus.serde.BinaryDeserializer;
import com.timeplus.serde.BinarySerializer;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;

// I see some binary protocol frameworks such as Protobuf chose an alternative way to represent UInt64 by long,
// and use special tools to calculate it. Since currently we don't guarantee any stable APIs except JDBC APIs,
// so we have an opportunity to change it later.
public class DataTypeUInt256 implements BaseDataTypeInt64<BigInteger, BigInteger>, BytesHelper {

    @Override
    public String name() {
        return "uint256";
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
        return 78;  //use the origin precision
    }

    @Override
    public void serializeBinary(BigInteger data, BinarySerializer serializer) throws SQLException, IOException {
        serializer.writeBigInteger(data, 32);  //seem to be lack of boundary check
    }

    @Override
    public BigInteger deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        byte[] bytes = deserializer.readBytes(32);
        return new BigInteger(1, deserializer.reversesBytes(bytes));  // force it to be positive
    }

    @Override
    public String[] getAliases() {
        return new String[0];
    }

    @Override
    public BigInteger deserializeText(SQLLexer lexer) throws SQLException {
        String Uinteger256 = lexer.stringLiteral();
        return new BigInteger(Uinteger256, 10);
    }

}
