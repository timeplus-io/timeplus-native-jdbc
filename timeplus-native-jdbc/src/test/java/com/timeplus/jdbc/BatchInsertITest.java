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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.Test;

public class BatchInsertITest extends AbstractITest {

    void assertBatchInsertResult(int[] result, int expectedRowCount) {
        assertEquals(expectedRowCount, result.length);
        assertEquals(expectedRowCount, Arrays.stream(result).sum());
    }

    @Test
    public void successfullyBatchInsert() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS test");
            statement.execute("CREATE STREAM test(id int8, age uint8, name string, name2 string) ENGINE=Memory");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test(id, age, name, name2) VALUES(?, 1, ?, ?)", pstmt -> {
                for (int i = 0; i < Byte.MAX_VALUE; i++) {
                    pstmt.setByte(1, (byte) i);
                    pstmt.setString(2, "Zhang San" + i);
                    pstmt.setString(3, "张三" + i);
                    pstmt.addBatch();
                }
                assertBatchInsertResult(pstmt.executeBatch(), Byte.MAX_VALUE);
            });
            ResultSet rs = statement.executeQuery("select * from test");
            boolean hasResult = false;
            for (int i = 0; i < Byte.MAX_VALUE && rs.next(); i++) {
                hasResult = true;
                assertEquals(i, rs.getByte(1));
                assertEquals(1, rs.getByte(2));
                assertEquals("Zhang San" + i, rs.getString(3));
                assertEquals("张三" + i, rs.getString(4));
            }
            assertTrue(hasResult);
        });

    }

    @Test
    public void successfullyMultipleBatchInsert() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS test");
            statement.execute("CREATE STREAM test(id int8, age uint8, name string)ENGINE=Memory");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test(id, age, name) VALUES(?, 1, ?)", pstmt -> {
                int insertBatchSize = 100;

                for (int i = 0; i < insertBatchSize; i++) {
                    pstmt.setByte(1, (byte) i);
                    pstmt.setString(2, "Zhang San" + i);
                    pstmt.addBatch();
                }
                assertBatchInsertResult(pstmt.executeBatch(), insertBatchSize);

                for (int i = 0; i < insertBatchSize; i++) {
                    pstmt.setByte(1, (byte) i);
                    pstmt.setString(2, "Zhang San" + i);
                    pstmt.addBatch();
                }
                assertBatchInsertResult(pstmt.executeBatch(), insertBatchSize);
            });
        });

    }

    @Test
    public void successfullyNullableDataType() throws Exception {
        withStatement(stmt -> {
            int insertBatchSize = 100;

            stmt.executeQuery("DROP STREAM IF EXISTS test");
            stmt.executeQuery("CREATE STREAM test(day date, name nullable(string), name2 nullable(fixed_string(10)) ) Engine=Memory");

            withPreparedStatement(stmt.getConnection(), "INSERT INTO test(day, name, name2) VALUES(?, ?, ?)", pstmt -> {
                for (int i = 0; i < insertBatchSize; i++) {
                    pstmt.setDate(1, new Date(System.currentTimeMillis()));

                    if (i % 2 == 0) {
                        pstmt.setString(2, "String");
                        pstmt.setString(3, "String");
                    } else {
                        pstmt.setString(2, null);
                        pstmt.setString(3, null);
                    }
                    pstmt.addBatch();
                }
                assertBatchInsertResult(pstmt.executeBatch(), insertBatchSize);
            });
            ResultSet rs = stmt.executeQuery("select name, name2 from test order by name");
            int i = 0;
            while (rs.next()) {
                String name1 = rs.getString(1);
                String name2 = rs.getString(2);

                if (i * 2 >= insertBatchSize) {
                    assertNull(name1);
                } else {
                    assertEquals("String", name1);
                    assertTrue(name2.contains("String"));
                    assertEquals(10, name2.length());
                }
                i++;
            }

            rs = stmt.executeQuery(
                    "select count_if(is_null(name)), count_if(is_not_null(name)), count_if(is_not_null(name2)) from test;");
            assertTrue(rs.next());
            assertEquals(insertBatchSize / 2, rs.getInt(1));
            assertEquals(insertBatchSize / 2, rs.getInt(2));
            assertEquals(insertBatchSize / 2, rs.getInt(3));

            stmt.executeQuery("DROP STREAM IF EXISTS test");
        });
    }

    @Test
    public void successfullyBatchInsertArray() throws Exception {
        System.setProperty("illegal-access", "allow");

        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS test");
            statement.execute("CREATE STREAM test(value0 array(string), value1 array(float64), value2 array(array(int32)), array3 array(nullable(float64)))ENGINE=Memory");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test(value0, value1, value2, array3) VALUES(?, ?, [[1,2,3]], ?)", pstmt -> {
                List<String> array0 = Arrays.asList("aa", "bb", "cc");
                List<Double> array1 = Arrays.asList(1.2, 2.2, 3.2);
                List<Double> array3 = Arrays.asList(1.2, 2.2, 3.2, null);

                for (int i = 0; i < Byte.MAX_VALUE; i++) {
                    pstmt.setArray(1, pstmt.getConnection().createArrayOf("string", array0.toArray()));
                    pstmt.setArray(2, pstmt.getConnection().createArrayOf("float64", array1.toArray()));
                    pstmt.setArray(3, pstmt.getConnection().createArrayOf("nullable(float64)", array3.toArray()));
                    pstmt.addBatch();
                }

                assertBatchInsertResult(pstmt.executeBatch(), Byte.MAX_VALUE);

                ResultSet rs = statement.executeQuery("select * from test");
                while (rs.next()) {
                    assertArrayEquals(array0.toArray(), (Object[]) rs.getArray(1).getArray());
                    assertArrayEquals(array1.toArray(), (Object[]) rs.getArray(2).getArray());
                    assertArrayEquals(array3.toArray(), (Object[]) rs.getArray(4).getArray());
                }
            });
        });

    }

    @Test
    public void successfullyBatchInsertDateTime() throws Exception {
        withStatement(statement -> {
            // 2018-07-01 00:00:00 Asia/Shanghai
            long time = 1530374400;

            statement.execute("DROP STREAM IF EXISTS test");
            statement.execute("CREATE STREAM test(time datetime)ENGINE=Memory");

            withPreparedStatement(statement.getConnection(), "INSERT INTO test VALUES(?)", pstmt -> {
                long insertTime = time;
                for (int i = 0; i < 24; i++) {
                    pstmt.setTimestamp(1, new Timestamp(insertTime * 1000));
                    pstmt.addBatch();
                    insertTime += 3600;
                }
                assertBatchInsertResult(pstmt.executeBatch(), 24);
            });

            long selectTime = time;
            ResultSet rs = statement.executeQuery("SELECT * FROM test ORDER BY time ASC");
            while (rs.next()) {
                assertEquals(selectTime * 1000, rs.getTimestamp(1).getTime());
                selectTime += 3600;
            }
        });

    }
    
    @Test
    public void successfullyBatchInsertMap() throws Exception {
        String[] valueTypes = new String[]{"uint32", "string", "tuple(int32, string)", "array(uint16)"};
        for (int j = 0; j < valueTypes.length; j++) {
            final int m = j;
            String eachType = valueTypes[j];
            withStatement(statement -> {
                statement.execute("SET allow_experimental_map_type = 1");
                statement.execute("DROP STREAM IF EXISTS test");
                statement.execute("CREATE STREAM test(tags map(string, " + eachType + "))ENGINE=Memory");

                withPreparedStatement(statement.getConnection(), "INSERT INTO test(tags) VALUES(?)", pstmt -> {
                    for (int i = 1; i <= 10; i++) {
                        Map<String, Object> map = new HashMap<>();
                        Object value = null;
                        switch (m) {
                            case 0: {
                                value = (long) i;
                                break;
                            }
                            case 1: {
                                value = "value" + i;
                                break;
                            }
                            case 2: {
                                value = pstmt.getConnection().createStruct("Tuple", new Object[]{(Integer) i, "value" + i});
                                break;
                            }
                            case 3: {
                                List<Integer> list = new ArrayList<Integer>();
                                for (int n = i; n > 0; n--) {
                                    list.add(n);
                                }
                                value = pstmt.getConnection().createArrayOf("Integer", list.toArray());
                                break;
                            }
                            default: {
                                break;
                            }
                        }
                        map.put("key" + i, value);
                        pstmt.setObject(1, map);
                        pstmt.addBatch();
                    }
                    assertBatchInsertResult(pstmt.executeBatch(), 10);
                });

                ResultSet rs = statement.executeQuery("SELECT tags FROM test");
                while (rs.next()) {
                    Map<Object, Object> kv = (Map<Object, Object>) rs.getObject(1);
                    for (Entry<Object, Object> each : kv.entrySet()) {
                        int v = 0;
                        switch (m) {
                            case 0: {
                                v = ((Long) each.getValue()).intValue();
                                break;
                            }
                            case 1: {
                                String vStr = (String) each.getValue();
                                v = Integer.parseInt(vStr.substring(5));
                                break;
                            }
                            case 2: {
                                v = (Integer) (((TimeplusStruct) each.getValue()).getAttributes()[0]);
                                break;
                            }
                            case 3: {
                                v = (Integer) (((TimeplusArray) each.getValue()).getArray()[0]);
                                break;
                            }
                            default: {
                                break;
                            }
                        }
                        assertEquals(each.getKey(), "key" + v);
                    }
                }
            });
        }

    }
}
