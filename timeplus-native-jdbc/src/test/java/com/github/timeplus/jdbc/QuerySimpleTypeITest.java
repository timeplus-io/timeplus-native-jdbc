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

package com.github.timeplus.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

public class QuerySimpleTypeITest extends AbstractITest {

    @Test
    public void successfullyByName() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery(
                    "SELECT to_int8(" + Byte.MIN_VALUE + ") as a , to_uint8(" + Byte.MAX_VALUE + ") as b");

            assertTrue(rs.next());
            assertEquals(Byte.MIN_VALUE, rs.getByte("a"));
            assertEquals(Byte.MAX_VALUE, rs.getByte("b"));
        });
    }

    @Test
    public void successfullyBooleanColumn() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement
                    .executeQuery("SELECT to_uint8(" + 1 + "), to_uint8(" + 0 + ")");

            assertTrue(rs.next());
            assertEquals(Boolean.TRUE, rs.getBoolean(1));
            assertEquals(Boolean.FALSE, rs.getBoolean(2));
        });
    }

    @Test
    public void successfullyByteColumn() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery(
                    "SELECT to_int8(" + Byte.MIN_VALUE + "), to_uint8(" + Byte.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Byte.MIN_VALUE, rs.getByte(1));
            assertEquals(Byte.MAX_VALUE, rs.getByte(2));
        });
    }

    @Test
    public void successfullyShortColumn() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery(
                    "SELECT to_int16(" + Short.MIN_VALUE + "), to_uint16(" + Short.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Short.MIN_VALUE, rs.getShort(1));
            assertEquals(Short.MAX_VALUE, rs.getShort(2));
        });
    }

    @Test
    public void successfullyDateColumn() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement.executeQuery("SELECT to_date('2105-12-30') AS value, to_type_name(value)");

            assertTrue(rs.next());
            assertEquals(Date.valueOf("2105-12-30"), rs.getDate(1));
            assertEquals("date32", rs.getString(2));
        });
    }

    @Test
    public void successfullyIntColumn() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery(
                    "SELECT to_int32(" + Integer.MIN_VALUE + "), to_uint32(" + Integer.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Integer.MIN_VALUE, rs.getInt(1));
            assertEquals(Integer.MAX_VALUE, rs.getInt(2));
        });
    }

    @Test
    public void successfullyUIntColumn() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT to_uint32(" + Integer.MAX_VALUE + " + 128)");

            assertTrue(rs.next());
            assertEquals((long) Integer.MAX_VALUE + 128L, rs.getLong(1));
        });
    }

    @Test
    public void successfullyLongColumn() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement
                    .executeQuery("SELECT to_int64(" + Long.MIN_VALUE + "), to_uint64(" + Long.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Long.MIN_VALUE, rs.getLong(1));
            assertEquals(Long.MAX_VALUE, rs.getLong(2));
        });
    }

    @Test
    public void successfullyFloatColumn() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement
                    .executeQuery("SELECT to_float32(" + Float.MIN_VALUE + "), to_float32(" + Float.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Float.MIN_VALUE, rs.getFloat(1), 0.000000000001);
            assertEquals(Float.MAX_VALUE, rs.getFloat(2), 0.000000000001);
        });
    }

    @Test
    public void successfullyDoubleColumn() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT to_float64(4.9E-32), to_float64(" + Double.MAX_VALUE + ")");

            assertTrue(rs.next());
            assertEquals(Double.MIN_VALUE, rs.getDouble(1), 0.000000000001);
            assertEquals(Double.MAX_VALUE, rs.getDouble(2), 0.000000000001);
        });
    }

    @Test
    public void successfullyDate32Column() throws Exception {
        withNewConnection(connect -> {
            Statement statement = connect.createStatement();
            ResultSet rs = statement.executeQuery("SELECT to_date('1955-01-01') AS value, to_type_name(value)");

            assertTrue(rs.next());
            assertEquals(Date.valueOf("1955-01-01"), rs.getDate(1));
            assertEquals("date32", rs.getString(2));
        });

        withNewConnection(connection -> {
            try (Statement statement = connection.createStatement()) {

                statement.executeQuery("DROP STREAM IF EXISTS test");
                statement.executeQuery("CREATE STREAM test(a date32)ENGINE=Memory");

                try (PreparedStatement ps = connection.prepareStatement("INSERT INTO test VALUES(?)")) {
                    ps.setDate(1, Date.valueOf("1955-01-01"));
                    assertEquals(1, ps.executeUpdate());
                }

                try (PreparedStatement ps = connection.prepareStatement("SELECT * FROM test WHERE a = to_date(?)")) {
                    ps.setDate(1, Date.valueOf("1955-01-01"));
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(Date.valueOf("1955-01-01"), rs.getDate(1));
                        assertFalse(rs.next());
                    }
                }
                statement.executeQuery("DROP STREAM IF EXISTS test");
            }
        });
    }

    @Test
    public void successfullyUUIDColumn() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT materialize('01234567-89ab-cdef-0123-456789abcdef')");

            assertTrue(rs.next());
            assertEquals("01234567-89ab-cdef-0123-456789abcdef", rs.getString(1));
        });
    }

    @Test
    public void successfullyNothingColumn() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("select null as aa, null as bb");

            assertTrue(rs.next());
            assertEquals(null, rs.getObject(1));
            assertEquals(null, rs.getObject(2));
        });
    }

    @Test
    public void successfullyMetadata() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery(
                    "SELECT number as a1, to_string(number) as a2, now() as a3, today() as a4, to_date(today()) as a5 from numbers(1)");

            assertTrue(rs.next());
            ResultSetMetaData metaData = rs.getMetaData();
            assertEquals("a1", metaData.getColumnName(1));
            assertEquals("uint64", metaData.getColumnTypeName(1));
            assertEquals("java.math.BigInteger", metaData.getColumnClassName(1));

            assertEquals("a2", metaData.getColumnName(2));
            assertEquals("string", metaData.getColumnTypeName(2));
            assertEquals("java.lang.String", metaData.getColumnClassName(2));

            assertEquals("a3", metaData.getColumnName(3));
            assertEquals("datetime", metaData.getColumnTypeName(3));
            assertEquals("java.sql.Timestamp", metaData.getColumnClassName(3));

            assertEquals("a4", metaData.getColumnName(4));
            assertEquals("date", metaData.getColumnTypeName(4));
            assertEquals("java.sql.Date", metaData.getColumnClassName(4));

            assertEquals("a5", metaData.getColumnName(5));
            assertEquals("date32", metaData.getColumnTypeName(5));
            assertEquals("java.sql.Date", metaData.getColumnClassName(5));
        });
    }

    @Test
    public void successfullyStringDataTypeWithSingleQuote() throws Exception {
        withStatement(statement -> {

            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.executeQuery("CREATE STREAM test(test string)ENGINE=Memory");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test VALUES(?)", pstmt -> {
                pstmt.setString(1, "test_string with ' character");
                assertEquals(1, pstmt.executeUpdate());
            });

            withPreparedStatement(statement.getConnection(), "SELECT * FROM test WHERE test=?", pstmt -> {
                pstmt.setString(1, "test_string with ' character");
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("test_string with ' character", rs.getString(1));
                    assertFalse(rs.next());
                }
            });
            statement.executeQuery("DROP STREAM IF EXISTS test");
        });
    }
}
