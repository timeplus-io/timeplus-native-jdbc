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
import com.timeplus.jdbc.TimeplusResultSet;
import com.timeplus.misc.BytesHelper;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;



public class NullTypeTest extends AbstractITest implements BytesHelper {

    @Test
    public void testBoolean() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(bool), nullableValue2 nullable(bool)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setBoolean(1, true);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Boolean value = rs.getBoolean(1);
                assertEquals(value, true);
                Object nullableValue = rs.getObject(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testUUID() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(uuid), nullableValue2 nullable(uuid)) Engine=Memory()");

            Integer rowCnt = 300;
            List<java.util.UUID> insertedValues = new ArrayList<>();
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    java.util.UUID uuid1 = java.util.UUID.randomUUID();
                    insertedValues.add(uuid1);
                    pstmt.setObject(1, uuid1);
                    pstmt.setNull(2, 2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                java.util.UUID value = (java.util.UUID) rs.getObject(1);
                java.util.UUID nullableValue = (java.util.UUID) rs.getObject(2);
                assertEquals(value, insertedValues.get(size - 1));
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testInt8() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(int8), nullableValue2 nullable(int8)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Object nullableValue = rs.getObject(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testInt16() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(int16), nullableValue2 nullable(int16)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Object nullableValue = rs.getObject(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testInt32() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(int32), nullableValue2 nullable(int32)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Object nullableValue = rs.getObject(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testInt64() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(int64), nullableValue2 nullable(int64)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setLong(1, 1);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Long value = rs.getLong(1);
                assertEquals(value, 1l);
                Object nullableValue = rs.getObject(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testInt128() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(int128), nullableValue2 nullable(int128)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    BigInteger value = new BigInteger("12345678901234567890123456789012");
                    pstmt.setObject(1, value, Types.BIGINT);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            TimeplusResultSet rs = (TimeplusResultSet) statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigInteger value = rs.getBigInteger(1);
                assertEquals(value, new BigInteger("12345678901234567890123456789012"));
                BigInteger nullableValue = rs.getBigInteger(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testInt256() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(int256), nullableValue2 nullable(int256)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    BigInteger value = new BigInteger("12345678901234567890123456789012");
                    pstmt.setObject(1, value, Types.BIGINT);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            TimeplusResultSet rs = (TimeplusResultSet) statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigInteger value = rs.getBigInteger(1);
                assertEquals(value, new BigInteger("12345678901234567890123456789012"));
                BigInteger nullableValue = rs.getBigInteger(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testUInt8() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(uint8), nullableValue2 nullable(uint8)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Object nullableValue = rs.getObject(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testUInt16() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(uint16), nullableValue2 nullable(uint16)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Object nullableValue = rs.getObject(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testUInt32() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(uint32), nullableValue2 nullable(uint32)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setInt(1, 1);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);
                Object nullableValue = rs.getObject(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testUInt64() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(uint64), nullableValue2 nullable(uint64)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setLong(1, 1);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Long value = rs.getLong(1);
                assertEquals(value, 1l);
                Object nullableValue = rs.getObject(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testUInt128() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(uint128), nullableValue2 nullable(uint128)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    BigInteger value = new BigInteger("12345678901234567890123456789012");
                    pstmt.setObject(1, value, Types.BIGINT);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            TimeplusResultSet rs = (TimeplusResultSet) statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigInteger value = rs.getBigInteger(1);
                assertEquals(value, new BigInteger("12345678901234567890123456789012"));
                BigInteger nullableValue = rs.getBigInteger(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testUInt256() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(uint256), nullableValue2 nullable(uint256)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    BigInteger value = new BigInteger("12345678901234567890123456789012");
                    pstmt.setObject(1, value, Types.BIGINT);
                    pstmt.setNull(2, 100);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            TimeplusResultSet rs = (TimeplusResultSet) statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigInteger value = rs.getBigInteger(1);
                assertEquals(value, new BigInteger("12345678901234567890123456789012"));
                BigInteger nullableValue = rs.getBigInteger(2);
                assertNull(nullableValue);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testStringType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute("CREATE STREAM IF NOT EXISTS Null_test (nullableValue1 nullable(string), nullableValue2 nullable(string)) Engine=Memory()");
 
            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (nullableValue1, nullableValue2) values(?, ?);")) {
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setNull(1, 1);
                    pstmt.setNull(2, 1);
                    pstmt.addBatch();
                }
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setString(1, "test");
                    pstmt.setString(2, "abcdefghij");
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
 
            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                if (size <= rowCnt / 2) {
                    String value = rs.getString(1);
                    assertNull(value);
                    String nullableValue = rs.getString(2);
                    assertNull(nullableValue);
                }
                else {
                    String value = rs.getString(1);
                    assertEquals(value, "test");
                    String nullableValue = rs.getString(2);
                    assertEquals(nullableValue, "abcdefghij");
                }
                
            }
 
            assertEquals(size, rowCnt);
 
            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testDateTypeNull() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS Null_test (date_value nullable(date), value_date32 nullable(date32), datetime_value nullable(datetime), value_datetime64 nullable(datetime64(3))) Engine=Memory()");

            Integer rowCnt = 300;
            Date temp1 = java.sql.Date.valueOf("2021-01-01");
            Date temp2 = java.sql.Date.valueOf("2021-01-02");
            Timestamp temp3 = java.sql.Timestamp.valueOf("2021-01-01 00:00:00");
            Timestamp temp4 = java.sql.Timestamp.valueOf("2021-01-02 00:00:00");
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (date_value, value_date32, datetime_value, value_datetime64) values(?,?,?,?);")) {
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setNull(1, 1);
                    pstmt.setNull(2, 1);
                    pstmt.setNull(3, 1);
                    pstmt.setNull(4, 1);
                    pstmt.addBatch();
                }
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setDate(1, temp1);
                    pstmt.setDate(2, temp2);
                    pstmt.setTimestamp(3, temp3);
                    pstmt.setTimestamp(4, temp4);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                if (size <= rowCnt / 2) {
                    Date dateValue = rs.getDate("date_value");
                    assertNull(dateValue);
                    Date dateValue32 = rs.getDate("value_date32");
                    assertNull(dateValue32);
                    Timestamp datetime = rs.getTimestamp("datetime_value");
                    assertNull(datetime);
                    Timestamp datetime64 = rs.getTimestamp("value_datetime64");
                    assertNull(datetime64);
                }
                else {
                    Date dateValue = rs.getDate("date_value");
                    assertEquals(dateValue, temp1);
                    Date dateValue32 = rs.getDate("value_date32");
                    assertEquals(dateValue32, temp2);
                    Timestamp datetime = rs.getTimestamp("datetime_value");
                    assertEquals(datetime, temp3);
                    Timestamp datetime64 = rs.getTimestamp("value_datetime64");
                    assertEquals(datetime64, temp4);
                }
                
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testDecimalTypeNull() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS Null_test (decimal32 nullable(decimal(5, 3)), decimal64 nullable(decimal(15, 5)), decimal128 nullable(decimal(38, 16)), decimal256 nullable(decimal(76, 26))) Engine=Memory()");

            Integer rowCnt = 300;
            BigDecimal temp1 = BigDecimal.valueOf(412341.21D).setScale(3, RoundingMode.HALF_UP);
            BigDecimal temp2 = BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP);
            BigDecimal temp3 = BigDecimal.valueOf(412341.21D).setScale(16, RoundingMode.HALF_UP);
            BigDecimal temp4 = BigDecimal.valueOf(412341.21D).setScale(26, RoundingMode.HALF_UP);
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (decimal32, decimal64, decimal128, decimal256) values(?,?,?,?);")) {
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setNull(1, 1);
                    pstmt.setNull(2, 1);
                    pstmt.setNull(3, 1);
                    pstmt.setNull(4, 1);
                    pstmt.addBatch();
                }
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setBigDecimal(1, temp1);
                    pstmt.setBigDecimal(2, temp2);
                    pstmt.setBigDecimal(3, temp3);
                    pstmt.setBigDecimal(4, temp4);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                if (size <= rowCnt / 2) {
                    BigDecimal rsValue32 = rs.getBigDecimal(1);
                    assertNull(rsValue32);
                    BigDecimal rsValue64 = rs.getBigDecimal(2);
                    assertNull(rsValue64);
                    BigDecimal rsValue128 = rs.getBigDecimal(3);
                    assertNull(rsValue128);
                    BigDecimal rsValue256 = rs.getBigDecimal(4);
                    assertNull(rsValue256);
                }
                else {
                    BigDecimal rsValue32 = rs.getBigDecimal(1);
                    assertEquals(temp1, rsValue32);
                    BigDecimal rsValue64 = rs.getBigDecimal(2);
                    assertEquals(temp2, rsValue64);
                    BigDecimal rsValue128 = rs.getBigDecimal(3);
                    assertEquals(temp3, rsValue128);
                    BigDecimal rsValue256 = rs.getBigDecimal(4);
                    assertEquals(temp4, rsValue256);
                }
                
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testEnumTypeNull() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS Null_test (enum8_value nullable(enum8('test','test2')), enum16_value nullable(enum16('test','test2'))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (enum8_value, enum16_value) values(?,?);")) {
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setNull(1, 1);
                    pstmt.setNull(2, 1);
                    pstmt.addBatch();
                }
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setString(1, "test");
                    pstmt.setString(2, "test");
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                if (size <= rowCnt / 2) {
                    String enum8_value = rs.getString(1);
                    assertNull(enum8_value);
                    String enum16_value = rs.getString(2);
                    assertNull(enum16_value);
                }
                else {
                    String enum8_value = rs.getString(1);
                    assertEquals("test", enum8_value);
                    String enum16_value = rs.getString(2);
                    assertEquals("test", enum16_value);
                }
                
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testFloatTypeNull() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS Null_test (float32_value nullable(float32), float64_value nullable(float64)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (float32_value, float64_value) values(?,?);")) {
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setNull(1, 1);
                    pstmt.setNull(2, 1);
                    pstmt.addBatch();
                }
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setFloat(1, 1.1f);
                    pstmt.setDouble(2, 100.1);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                if (size <= rowCnt / 2) {
                    Object float32_value = rs.getObject(1);
                    assertNull(float32_value);
                    Object float64_value = rs.getObject(2);
                    assertNull(float64_value);
                }
                else {
                    Float float32_value = rs.getFloat(1);
                    assertEquals(1.1f, float32_value);
                    Double float64_value = rs.getDouble(2);
                    assertEquals(100.1, float64_value);
                }
                
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    @Test
    public void testIPTypeNull() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS Null_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS Null_test (ipv4_value nullable(ipv4), ipv6_value nullable(ipv6)) Engine=Memory()");

            Integer rowCnt = 300;
            Long testIPv4Value1 = ipToLong("192.168.1.1");
            BigInteger testIPv6Value1 = new BigInteger("20010db885a3000000008a2e03707334", 16);
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO Null_test (ipv4_value, ipv6_value) values(?,?);")) {
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setNull(1, 1);
                    pstmt.setNull(2, 1);
                    pstmt.addBatch();
                }
                for (int i = 0; i < rowCnt / 2; i++) {
                    pstmt.setLong(1, testIPv4Value1);
                    pstmt.setObject(2, testIPv6Value1, Types.BIGINT);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            TimeplusResultSet rs = (TimeplusResultSet) statement.executeQuery("SELECT * FROM Null_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                if (size <= rowCnt / 2) {
                    Object ipv4_value = rs.getObject(1);
                    assertNull(ipv4_value);
                    Object ipv6_value = rs.getObject(2);
                    assertNull(ipv6_value);
                }
                else {
                    Long ipv4_value = rs.getLong(1);
                    assertEquals(testIPv4Value1, ipv4_value);
                    BigInteger ipv6_value = rs.getBigInteger(2);
                    assertEquals(testIPv6Value1, ipv6_value);
                }
                
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS Null_test");
        });
    }

    public long ipToLong(String ipAddress) {
        String[] ipAddressInArray = ipAddress.split("\\.");

        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {
            int power = 3 - i;
            int ip = Integer.parseInt(ipAddressInArray[i]);
            result += ip * Math.pow(256, power);
        }

        return result;
    }

}
