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

package com.timeplus.jdbc.type;

import com.timeplus.jdbc.AbstractITest;
import com.timeplus.misc.BytesHelper;
import org.junit.jupiter.api.Test;
import com.timeplus.jdbc.TimeplusResultSet;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import static org.junit.jupiter.api.Assertions.assertEquals;



public class IntegerTypeTest extends AbstractITest implements BytesHelper {

    @Test
    public void testUint8Type() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS uint8_test");
            statement.execute("CREATE STREAM IF NOT EXISTS uint8_test (value uint8, nullableValue nullable(uint8)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO uint8_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setInt(2, 2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM uint8_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Integer nullableValue = rs.getInt(2);
                assertEquals(nullableValue, 2);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS uint8_test");
        });
    }

    @Test
    public void testUint16() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS uint16_test");
            statement.execute("CREATE STREAM IF NOT EXISTS uint16_test (value uint16, nullableValue nullable(uint16)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO uint16_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setInt(2, 2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM uint16_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Integer nullableValue = rs.getInt(2);
                assertEquals(nullableValue, 2);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS uint16_test");
        });
    }

    @Test
    public void testUint32() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS uint32_test");
            statement.execute("CREATE STREAM IF NOT EXISTS uint32_test (value uint32, nullableValue nullable(uint32)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO uint32_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setLong(1, 1);
                    pstmt.setLong(2, 2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM uint32_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Long value = rs.getLong(1);
                assertEquals(value, 1);
                Long nullableValue = rs.getLong(2);
                assertEquals(nullableValue, 2);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS uint32_test");
        });
    }

    @Test
    public void testUint64() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS uint64_test");
            statement.execute("CREATE STREAM IF NOT EXISTS uint64_test (value uint64, nullableValue nullable(uint64)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO uint64_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setLong(1, 1);
                    pstmt.setLong(2, 2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM uint64_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Long value = rs.getLong(1);
                assertEquals(value, 1);
                Long nullableValue = rs.getLong(2);
                assertEquals(nullableValue, 2);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS uint64_test");
        });
    }

    @Test
    public void testInt8() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS int8_test");
            statement.execute("CREATE STREAM IF NOT EXISTS int8_test (value int8, nullableValue nullable(int8)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO int8_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setInt(2, 2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM int8_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Integer nullableValue = rs.getInt(2);
                assertEquals(nullableValue, 2);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS int8_test");
        });
    }

    @Test
    public void testInt16() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS int16_test");
            statement.execute("CREATE STREAM IF NOT EXISTS int16_test (value int16, nullableValue nullable(int16)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO int16_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setInt(2, 2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM int16_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Integer nullableValue = rs.getInt(2);
                assertEquals(nullableValue, 2);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS int16_test");
        });
    }

    @Test
    public void testInt32() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS int32_test");
            statement.execute("CREATE STREAM IF NOT EXISTS int32_test (value int32, nullableValue nullable(int32)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO int32_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setInt(2, 2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM int32_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Integer nullableValue = rs.getInt(2);
                assertEquals(nullableValue, 2);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS int32_test");
        });
    }

    @Test
    public void testInt64() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS int64_test");
            statement.execute("CREATE STREAM IF NOT EXISTS int64_test (value int64, nullableValue nullable(int64)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO int64_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setLong(1, 1);
                    pstmt.setLong(2, 2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM int64_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Long value = rs.getLong(1);
                assertEquals(value, 1);
                Long nullableValue = rs.getLong(2);
                assertEquals(nullableValue, 2);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS int64_test");
        });
    }

    @Test
    public void testInt128() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS int128_test");
            statement.execute("CREATE STREAM IF NOT EXISTS int128_test (value int128, nullableValue nullable(int128)) Engine=Memory()");
            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO int128_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    BigInteger value = new BigInteger("12345678901234567890123456789012");
                    BigInteger nullableValue = new BigInteger("2");
                    pstmt.setObject(1, value, Types.BIGINT);
                    pstmt.setObject(2, nullableValue, Types.BIGINT);
                    
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            TimeplusResultSet rs = (TimeplusResultSet) statement.executeQuery("SELECT * FROM int128_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigInteger value = rs.getBigInteger(1);
                BigInteger nullableValue = rs.getBigInteger(2);
                assertEquals(value, new BigInteger("12345678901234567890123456789012"));
                assertEquals(nullableValue, new BigInteger("2"));
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS int128_test");
        });
    }

    @Test
    public void testInt256() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS int256_test");
            statement.execute("CREATE STREAM IF NOT EXISTS int256_test (value int256, nullableValue nullable(int256)) Engine=Memory()");
            Integer rowCnt = 10;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO int256_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    BigInteger value = new BigInteger("1234567890123456789012345678901212345678901234567890123456789012");
                    BigInteger nullableValue = new BigInteger("-210987654321098");
                    pstmt.setObject(1, value, Types.BIGINT);
                    pstmt.setObject(2, nullableValue, Types.BIGINT);

                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            TimeplusResultSet rs = (TimeplusResultSet) statement.executeQuery("SELECT * FROM int256_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigInteger value = rs.getBigInteger(1);
                BigInteger nullableValue = rs.getBigInteger(2);
                assertEquals(value, new BigInteger("1234567890123456789012345678901212345678901234567890123456789012"));
                assertEquals(nullableValue, new BigInteger("-210987654321098"));
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS int256_test");
        });
    }

    @Test
    public void testUint128() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS uint128_test");
            statement.execute("CREATE STREAM IF NOT EXISTS uint128_test (value uint128, nullableValue nullable(uint128)) Engine=Memory()");
            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO uint128_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    BigInteger value = new BigInteger("12345678901234567890123456789012");
                    BigInteger nullableValue = new BigInteger("2");
                    pstmt.setObject(1, value, Types.BIGINT);
                    pstmt.setObject(2, nullableValue, Types.BIGINT);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            TimeplusResultSet rs = (TimeplusResultSet) statement.executeQuery("SELECT * FROM uint128_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigInteger value = rs.getBigInteger(1);
                BigInteger nullableValue = rs.getBigInteger(2);
                assertEquals(value, new BigInteger("12345678901234567890123456789012"));
                assertEquals(nullableValue, new BigInteger("2"));
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS uint128_test");
        });
    }    

    @Test
    public void testUint256() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS uint256_test");
            statement.execute("CREATE STREAM IF NOT EXISTS uint256_test (value uint256, nullableValue nullable(uint256)) Engine=Memory()");
            Integer rowCnt = 10;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO uint256_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    BigInteger value = new BigInteger("1234567890123456789012345678901212345678901234567890123456789012");
                    BigInteger nullableValue = new BigInteger("9876543210987654321098765432109898765432109876543210987654321098");
                    pstmt.setObject(1, value, Types.BIGINT);
                    pstmt.setObject(2, nullableValue, Types.BIGINT);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            TimeplusResultSet rs = (TimeplusResultSet) statement.executeQuery("SELECT * FROM uint256_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigInteger value = rs.getBigInteger(1);
                BigInteger nullableValue = rs.getBigInteger(2);
                assertEquals(value, new BigInteger("1234567890123456789012345678901212345678901234567890123456789012"));
                assertEquals(nullableValue, new BigInteger("9876543210987654321098765432109898765432109876543210987654321098"));
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS uint256_test");
        });
    }    
}
