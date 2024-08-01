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
import com.timeplus.jdbc.TimeplusStruct;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.sql.Array;


import static org.junit.jupiter.api.Assertions.assertEquals;

public class NestedTypeTest extends AbstractITest implements BytesHelper {

    // test for nested type, array of array, array of tuple, tuple of array, tuple of tuple, nullable type involved
    @Test
    public void testNestedArrayTupleType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS nested_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS nested_test (value array(array(low_cardinality(nullable(int)))), "
                    +"value2 array(tuple(low_cardinality(nullable(int)), low_cardinality(nullable(string)))), "
                    +"value3 tuple(array(low_cardinality(nullable(int))), low_cardinality(nullable(string)))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO nested_test (value, value2, value3) values(?, ?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    // array of array
                    Array innerArray1 = statement.getConnection().createArrayOf("low_cardinality(nullable(int32))", new Object[]{1, 2, null});
                    Array innerArray2 = statement.getConnection().createArrayOf("low_cardinality(nullable(int32))", new Object[]{4, 5, null});
                    pstmt.setObject(1, statement.getConnection().createArrayOf("array(low_cardinality(nullable(int32)))", new Object[]{innerArray1, innerArray2}));
                    
                    // array of tuple
                    TimeplusStruct tuple1 = new TimeplusStruct("tuple", new Object[]{1, null});
                    TimeplusStruct tuple2 = new TimeplusStruct("tuple", new Object[]{null, "test2"});
                    pstmt.setObject(2, statement.getConnection().createArrayOf("tuple(low_cardinality(nullable(int)), low_cardinality(nullable(string)))", new Object[]{tuple1, tuple2}));

                    // tuple (array, string)
                    Array array = statement.getConnection().createArrayOf("low_cardinality(nullable(int))", new Integer[]{1, 2, null});
                    TimeplusStruct tuple3 = new TimeplusStruct("tuple(array(low_cardinality(nullable(int))), low_cardinality(nullable(string)))", new Object[]{array, null});
                    pstmt.setObject(3, tuple3);

                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM nested_test;");
           // System.out.println("rs: " + rs.getRow());
            int size = 0;
            while (rs.next()) {
                size++;
                // array of array
                Array array = rs.getArray(1);
                Object[] arrayValue = (Object[]) array.getArray();
                assertEquals(arrayValue.length, 2);
                Array innerArray1 = (Array) arrayValue[0];
                Object[] innerArrayValue1 = (Object[]) innerArray1.getArray();
                assertEquals(innerArrayValue1.length, 3);
                assertEquals(innerArrayValue1[0], 1);
                assertEquals(innerArrayValue1[1], 2);
                assertEquals(innerArrayValue1[2], null);
                Array innerArray2 = (Array) arrayValue[1];
                Object[] innerArrayValue2 = (Object[]) innerArray2.getArray();
                assertEquals(innerArrayValue2.length, 3);
                assertEquals(innerArrayValue2[0], 4);
                assertEquals(innerArrayValue2[1], 5);
                assertEquals(innerArrayValue2[2], null);
                
                // array of tuple
                Array array2 = rs.getArray(2);
                Object[] arrayValue2 = (Object[]) array2.getArray();
                assertEquals(arrayValue2.length, 2);
                TimeplusStruct tuple1 = (TimeplusStruct) arrayValue2[0];
                Object[] tupleValue1 = (Object[]) tuple1.getAttributes();
                assertEquals(tupleValue1[0], 1);
                assertEquals(tupleValue1[1], null);
                TimeplusStruct tuple2 = (TimeplusStruct) arrayValue2[1];
                Object[] tupleValue2 = (Object[]) tuple2.getAttributes();
                assertEquals(tupleValue2[0], null);
                assertEquals(tupleValue2[1], "test2");

                // tuple (array, string)
                TimeplusStruct tuple3 = (TimeplusStruct) rs.getObject(3);
                Object[] tupleValue3 = (Object[]) tuple3.getAttributes();
                Array array3 = (Array) tupleValue3[0];
                Object[] arrayValue3 = (Object[]) array3.getArray();
                assertEquals(arrayValue3.length, 3);
                assertEquals(arrayValue3[0], 1);
                assertEquals(arrayValue3[1], 2);
                assertEquals(arrayValue3[2], null);
                assertEquals(tupleValue3[1], null);

            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS nested_test");
        });
    }

    @Test
    public void testNestedArrayMapType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS nested_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS nested_test (value array(map(int32, low_cardinality(nullable(string)))), "
                    +"value2 map(string, array(low_cardinality(nullable(int))))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO nested_test (value, value2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    // array of map
                    Map<Integer, String> map1 = new HashMap<>();
                    map1.put(1, null);
                    map1.put(2, "test");
                    Map<Integer, String> map2 = new HashMap<>();
                    map2.put(1, null);
                    map2.put(2, null);
                    pstmt.setObject(1, statement.getConnection().createArrayOf("map(int32, low_cardinality(nullable(string)))", new Object[]{map1, map2}));

                    // map (string, array)
                    Array array = statement.getConnection().createArrayOf("low_cardinality(nullable(int))", new Integer[]{1, 2, null});
                    Map<String, Object> map3 = new HashMap<>();
                    map3.put("array", array);
                    pstmt.setObject(2, map3);

                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM nested_test;");
           // System.out.println("rs: " + rs.getRow());
            int size = 0;
            while (rs.next()) {
                size++; 
                // array of map
                Array array2 = rs.getArray(1);
                Object[] arrayValue2 = (Object[]) array2.getArray();
                assertEquals(arrayValue2.length, 2);
                Map<Integer, String> map1 = (Map<Integer, String>) arrayValue2[0];
                assertEquals(map1.get(1), null);
                assertEquals(map1.get(2), "test");
                Map<Integer, String> map2 = (Map<Integer, String>) arrayValue2[1];
                assertEquals(map2.get(1), null);
                assertEquals(map2.get(2), null);

                // map (string, array)
                Map<String, Object> map3 = (Map<String, Object>) rs.getObject(2);
                Array array3 = (Array) map3.get("array");
                Object[] arrayValue3 = (Object[]) array3.getArray();
                assertEquals(arrayValue3.length, 3);
                assertEquals(arrayValue3[0], 1);
                assertEquals(arrayValue3[1], 2);
                assertEquals(arrayValue3[2], null);

            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS nested_test");
        });
    }

    @Test
    public void testNestedTupleMapType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS nested_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS nested_test (value tuple(map(int32, low_cardinality(nullable(string))), low_cardinality(nullable(string))), "
                    +"value2 map(string, tuple(low_cardinality(nullable(int))))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO nested_test (value, value2) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    // tuple (map, string)
                    Map<Integer, String> map1 = new HashMap<>();
                    map1.put(1, null);
                    map1.put(2, "test");
                    Object[] tupleValue = new Object[]{map1, "test"};
                    TimeplusStruct tuple = new TimeplusStruct("tuple", tupleValue);
                    pstmt.setObject(1, tuple);

                    // map (string, tuple)
                    Object[] tupleValue2 = new Object[]{3};
                    TimeplusStruct tuple2 = new TimeplusStruct("tuple", tupleValue2);
                    Map<String, Object> map2 = new HashMap<>();
                    map2.put("tuple", tuple2);
                    pstmt.setObject(2, map2);

                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM nested_test;");
           // System.out.println("rs: " + rs.getRow());
            int size = 0;
            while (rs.next()) {
                size++; 
                // tuple (map, string)
                Object obj1 = rs.getObject(1);
                TimeplusStruct tuple = (TimeplusStruct) obj1;
                Object[] tupleValue = (Object[]) tuple.getAttributes();
                Map<Integer, String> map1 = (Map<Integer, String>) tupleValue[0];
                assertEquals(map1.get(1), null);
                assertEquals(map1.get(2), "test");
                assertEquals(tupleValue[1], "test");

                // map (string, tuple)
                Map<String, Object> map2 = (Map<String, Object>) rs.getObject(2);
                Object obj2 = (Object) map2.get("tuple");
                TimeplusStruct tuple2 = (TimeplusStruct) obj2;
                Object[] tupleValue2 = (Object[]) tuple2.getAttributes();
                assertEquals(tupleValue2.length, 1);
                assertEquals(tupleValue2[0], 3);

            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS nested_test");
        });
    }
   
}
