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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.Struct;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class QueryComplexTypeITest extends AbstractITest {

    @Test
    public void successfullyDate() throws Exception {
        LocalDate date = LocalDate.of(2020, 1, 1);

        // use client timezone, Asia/Shanghai
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("select to_date('2020-01-01') as dateValue");
            assertTrue(rs.next());
            assertEquals(date, rs.getDate(1).toLocalDate());
            assertFalse(rs.next());
        }, "use_client_time_zone", true);

        // use server timezone, UTC
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("select to_date('2020-01-01') as dateValue");
            assertTrue(rs.next());
            assertEquals(date, rs.getDate(1).toLocalDate());
            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyNullableWithDateTimeWithoutTimezone() throws Exception {
        withStatement(statement -> {
            long ts = 946659723 * 1000L;
            ResultSet rs = statement.executeQuery("SELECT null_if(to_datetime(946659723), to_datetime(0))");
            assertTrue(rs.next());
            assertEquals(ts, rs.getTimestamp(1).getTime());
            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyFixedString() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT to_fixed_string('abc',3),to_fixed_string('abc',4)");

            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("abc\u0000", rs.getString(2));
        });
    }

    @Test
    public void successfullyNullableDataType() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT array_join([NULL,1])");

            assertTrue(rs.next());
            assertEquals(0, rs.getByte(1));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertNotNull(rs.getObject(1));
        });
    }

    @Test
    public void successfullyNullableFixedStringType() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT array_join([NULL,to_fixed_string('abc',3)])");

            assertTrue(rs.next());
            assertNull(rs.getString(1));
            assertTrue(rs.wasNull());
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyArray() throws Exception {
        withStatement(statement -> {
            // Array(UInt8)
            ResultSet rs = statement.executeQuery("SELECT [[1], [2], [3], [4,5,6]] from numbers(10)");

            for (int i = 0; i < 10; i++) {
                assertTrue(rs.next());
                Array array1 = rs.getArray(1);
                Object[] objects = (Object[]) array1.getArray();
                assertEquals(4, objects.length);

                TimeplusArray a1 = (TimeplusArray) (objects[0]);
                TimeplusArray a2 = (TimeplusArray) (objects[1]);
                TimeplusArray a3 = (TimeplusArray) (objects[2]);
                TimeplusArray a4 = (TimeplusArray) (objects[3]);

                assertArrayEquals(new Short[]{(short) 1}, (Object[]) a1.getArray());
                assertArrayEquals(new Short[]{(short) 2}, (Object[]) a2.getArray());
                assertArrayEquals(new Short[]{(short) 3}, (Object[]) a3.getArray());
                assertArrayEquals(new Short[]{(short) 4, (short) 5, (short) 6}, (Object[]) a4.getArray());
            }
            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyArrayJoin() throws Exception {
        withStatement(statement -> {
            // Array(UInt8)
            ResultSet rs = statement.executeQuery("SELECT array_join([[1,2,3],[4,5]])");

            assertTrue(rs.next());
            Array array1 = rs.getArray(1);
            assertNotNull(array1);
            assertArrayEquals(new Short[]{(short) 1, (short) 2, (short) 3}, (Object[]) (array1.getArray()));

            assertTrue(rs.next());
            Array array2 = rs.getArray(1);
            assertNotNull(array2);
            assertArrayEquals(new Number[]{(short) 4, (short) 5}, (Object[]) array2.getArray());
        });
    }

    @Test
    public void successfullyArrayTuple() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT array_join([[(1,'3'), (2,'4')],[(3,'5')]])");

            assertTrue(rs.next());
            Array array1 = rs.getArray(1);
            assertNotNull(array1);

            Object[] row1 = (Object[]) array1.getArray();
            assertEquals(2, row1.length);
            assertEquals(1, ((Short) (((TimeplusStruct) row1[0]).getAttributes()[0])).intValue());
            assertEquals("3", ((TimeplusStruct) row1[0]).getAttributes()[1]);

            assertEquals(2, ((Short) (((TimeplusStruct) row1[1]).getAttributes()[0])).intValue());
            assertEquals("4", ((TimeplusStruct) row1[1]).getAttributes()[1]);

            assertTrue(rs.next());
            Array array2 = rs.getArray(1);
            Object[] row2 = (Object[]) array2.getArray();
            assertEquals(1, row2.length);
            assertEquals(3, ((Short) (((TimeplusStruct) row2[0]).getAttributes()[0])).intValue());
            assertEquals("5", (((TimeplusStruct) row2[0]).getAttributes()[1]));

            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyArrayArray() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery(
                    "SELECT [[1.1, 1.2], [2.1, 2.2], [3.1, 3.2]] AS v, to_type_name(v), [to_nullable(10000), to_nullable(10001)] from numbers(10)");

            for (int i = 0; i < 10; i++) {
                assertTrue(rs.next());
                Array array1 = rs.getArray(1);
                assertNotNull(array1);

                Double[][] res = new Double[][]{{1.1, 1.2}, {2.1, 2.2}, {3.1, 3.2}};

                Object[] arr = (Object[]) (rs.getArray(1).getArray());
                assertArrayEquals(res[0], (Object[]) ((TimeplusArray) (arr[0])).getArray());
                assertArrayEquals(res[1], (Object[]) ((TimeplusArray) (arr[1])).getArray());
                assertArrayEquals(res[2], (Object[]) ((TimeplusArray) (arr[2])).getArray());
                assertEquals("array(array(float64))", rs.getString(2));

                arr = (Object[]) (rs.getArray(3).getArray());
                assertEquals(10000, arr[0]);
                assertEquals(10001, arr[1]);
            }
            assertFalse(rs.next());
        });
    }

    @Test
    public void successfullyTimestamp() throws Exception {
        withStatement(statement -> {
            long ts = 946659723 * 1000L;
            ResultSet rs = statement.executeQuery("SELECT to_datetime(946659723)");

            assertTrue(rs.next());
            assertEquals(ts, rs.getTimestamp(1).getTime());
        });
    }

    @Test
    public void successfullyNothing() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT array_cast()");
            assertTrue(rs.next());
            Array array = rs.getArray(1);
            assertEquals(array.getBaseTypeName(), "nothing");
        });
    }

    @Test
    public void successfullyNullableNothing() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT array_cast(null)");
            assertTrue(rs.next());
            Array array = rs.getArray(1);
            assertEquals(array.getBaseTypeName(), "nullable(nothing)");
        });
    }

    @Test
    public void successfullyTuple() throws Exception {
        withStatement(statement -> {
            ResultSet rs = statement.executeQuery("SELECT (to_uint32(1),'2')");

            assertTrue(rs.next());
            Struct struct = (Struct) rs.getObject(1);
            assertArrayEquals(new Object[]{(long) 1, "2"}, struct.getAttributes());

            Map<String, Class<?>> attrNameWithClass = new LinkedHashMap<>();
            attrNameWithClass.put("_2", String.class);
            attrNameWithClass.put("_1", Long.class);
            assertArrayEquals(new Object[]{"2", (long) 1}, struct.getAttributes(attrNameWithClass));
        });
    }

    @Test
    public void successfullyEnum8() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.execute("CREATE STREAM test (test enum8('a' = -1, 'b' = 1))ENGINE = Memory");
            statement.execute("INSERT INTO test(test) VALUES('a')");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");

            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());
            statement.executeQuery("DROP STREAM IF EXISTS test");
        });
    }

    @Test
    public void successfullyEnum16() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.execute("CREATE STREAM test (test enum16('a' = -1, 'b' = 1))ENGINE = Memory");
            statement.execute("INSERT INTO test(test) VALUES('a')");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");

            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());
            statement.executeQuery("DROP STREAM IF EXISTS test");
        });
    }
}
