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

package com.timeplus.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.Struct;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class InsertComplexTypeITest extends AbstractITest {

    @Test
    public void successfullyArrayDataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.executeQuery("CREATE STREAM test(test_Array array(uint8), test_Array2 array(array(string)), n3 array(nullable(uint8)) )ENGINE=Memory");
            statement.executeQuery("INSERT INTO test(test_Array, test_Array2, n3) VALUES ([1, 2, 3, 4], [ ['1', '2'] ], [1, 2, NULL] )");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            assertTrue(rs.next());
            assertArrayEquals(new Short[]{1, 2, 3, 4}, (Object[]) rs.getArray(1).getArray());
            Object[] objects = (Object[]) rs.getArray(2).getArray();
            TimeplusArray array = (TimeplusArray) objects[0];
            assertArrayEquals(new Object[]{"1", "2"}, array.getArray());

            objects = (Object[]) rs.getArray(3).getArray();
            assertArrayEquals(new Short[]{1, 2, null}, objects);

            assertFalse(rs.next());
            statement.executeQuery("DROP STREAM IF EXISTS test");
        });
    }

    @Test
    public void successfullyFixedStringDataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.executeQuery("CREATE STREAM test(str fixed_string(3))ENGINE=Memory");
            statement.executeQuery("INSERT INTO test(str) VALUES('abc')");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test(str) VALUES(?)", pstmt -> {
                pstmt.setObject(1, "abc");
                pstmt.executeUpdate();
            });

            ResultSet rs = statement.executeQuery("SELECT str, COUNT(0) FROM test group by str");
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals(2, rs.getInt(2));
            assertFalse(rs.next());
            statement.executeQuery("DROP STREAM IF EXISTS test");
        });
    }

    @Test
    public void successfullyNullableDataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.executeQuery("CREATE STREAM test(test_nullable nullable(uint8)) ENGINE=Memory");
            statement.executeQuery("INSERT INTO test(test_nullable) VALUES(Null)(1)(3)(Null)");
            ResultSet rs = statement.executeQuery("SELECT * FROM test ORDER BY test_nullable");
            assertTrue(rs.next());
            assertEquals(1, rs.getByte(1));
            assertTrue(rs.next());
            assertEquals(3, rs.getByte(1));
            assertTrue(rs.next());
            assertEquals(0, rs.getByte(1));
            assertTrue(rs.wasNull());
            assertEquals(0, rs.getByte(1));
            assertTrue(rs.wasNull());
            statement.executeQuery("DROP STREAM IF EXISTS test");
        });
    }

    @Test
    public void successfullyDateTimeDataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.executeQuery("CREATE STREAM test(test_datetime datetime('UTC'), test_datetime2 datetime('Asia/Shanghai') )ENGINE=Memory");
            statement.executeQuery("INSERT INTO test(test_datetime, test_datetime2) VALUES('2000-01-01 08:01:01', '2000-01-01 08:01:01')");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            assertTrue(rs.next());

            assertEquals(
                Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 8, 1, 1, 0)).getTime(),
                rs.getTimestamp(1).getTime());

            assertEquals(
                Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 8, 1, 1, 0)).getTime(),
                rs.getTimestamp(2).getTime());

            assertFalse(rs.next());
            statement.executeQuery("DROP STREAM IF EXISTS test");
        }, "use_client_time_zone", true);
    }

    @Test
    public void successfullyDateTime64DataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.executeQuery("CREATE STREAM test(seq uint8, test_datetime datetime64(9, 'UTC'))ENGINE=Memory");
            statement.executeQuery("INSERT INTO test(seq, test_datetime) VALUES(1, to_datetime64('2000-01-01 00:01:01.123456789'))");
            statement.executeQuery("INSERT INTO test(seq, test_datetime) VALUES(2, to_datetime64('2000-01-01 00:01:01.0234567'))");
            statement.executeQuery("INSERT INTO test(seq, test_datetime) VALUES(3, to_datetime64('2000-01-01 00:01:01.0234567889'))");
            ResultSet rs = statement.executeQuery("SELECT * FROM test ORDER BY seq");
            assertTrue(rs.next());
            assertEquals(Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 1, 1, 123456789)), rs.getTimestamp(2));
            assertTrue(rs.next());
            assertEquals(Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 1, 1, 23456700)), rs.getTimestamp(2));
            assertTrue(rs.next());
            assertEquals(Timestamp.valueOf(LocalDateTime.of(2000, 1, 1, 0, 1, 1, 23456789)), rs.getTimestamp(2));
            assertFalse(rs.next());
            statement.executeQuery("DROP STREAM IF EXISTS test");
        }, "use_client_time_zone", true);
    }

    @Test
    public void successfullyMinDateTime64DataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.executeQuery("CREATE STREAM test(test_datetime datetime64(9, 'UTC'))ENGINE=Memory");
            statement.executeQuery("INSERT INTO test(test_datetime) VALUES(to_datetime64('1970-01-01 00:00:00.000000000'))");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            assertTrue(rs.next());
            assertEquals(
                Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 0, 0, 0)),
                rs.getTimestamp(1));
            assertFalse(rs.next());
            statement.executeQuery("DROP STREAM IF EXISTS test");
        }, "use_client_time_zone", true);
    }

    @Test
    public void successfullyMaxDateTime64DataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.executeQuery("CREATE STREAM test(test_datetime datetime64(9, 'UTC'))ENGINE=Memory");
            statement.executeQuery("INSERT INTO test(test_datetime) VALUES(to_datetime64('2105-12-31 23:59:59.999999999'))");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            assertTrue(rs.next());

            assertEquals(
                Timestamp.valueOf(LocalDateTime.of(2105, 12, 31, 23, 59, 59, 999999999)),
                rs.getTimestamp(1));
            assertFalse(rs.next());
            statement.executeQuery("DROP STREAM IF EXISTS test");
        }, "use_client_time_zone", true);
    }

    @Test
    public void successfullyTupleDataType() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.executeQuery("CREATE STREAM test(test_tuple tuple(string, uint8),"
                                   + " tuple_array  tuple(array(nullable(string)), nullable(uint8)),"
                                   + " array_tuple array(tuple(uint32, nullable(string)) )"
                                   + " )ENGINE=Memory");
            statement.executeQuery("INSERT INTO test(test_tuple, tuple_array, array_tuple) VALUES( ('test_string', 1), (['1'], 32), [(32, '1'), (22, NULL) ] )");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            assertTrue(rs.next());
            assertArrayEquals(
                new Object[]{"test_string", (short) (1)},
                ((Struct) rs.getObject(1)).getAttributes());

            Object []objs = ((Struct) rs.getObject(2)).getAttributes();

            TimeplusArray arr = (TimeplusArray) (objs[0]);
            assertArrayEquals(new Object[]{"1"}, (Object[]) arr.getArray());
            assertEquals((short) 32, objs[1]);

            arr = (TimeplusArray) rs.getObject(3);

            TimeplusStruct t1 = (TimeplusStruct) ((Object[]) arr.getArray())[0];
            TimeplusStruct t2 = (TimeplusStruct) ((Object[]) arr.getArray())[1];
            assertArrayEquals(new Object[]{(long) 32, "1"}, t1.getAttributes());
            assertArrayEquals(new Object[]{(long) 22, null}, t2.getAttributes());

            assertFalse(rs.next());
            statement.executeQuery("DROP STREAM IF EXISTS test");
        });
    }
}
