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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.sql.Array;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NestedDataTypeTest extends AbstractITest implements BytesHelper {

    // test for nested type, array of array, array of tuple, tuple of array, tuple of tuple, nullable type involved
    @Test
    public void testNestedType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS nested_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS nested_test (value uint32, nested_value nested(name string, age int32)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO nested_test (value, nested_value.name, nested_value.age) values(?, ?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    Array innerArray1 = statement.getConnection().createArrayOf("string", new Object[]{"1", "2", "3"});
                    Array innerArray2 = statement.getConnection().createArrayOf("int32", new Object[]{4, 5, 6});
                    pstmt.setInt(1, 1);
                    pstmt.setObject(2, innerArray1);
                    pstmt.setObject(3, innerArray2);

                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM nested_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Integer value = rs.getInt(1);
                assertEquals(value, 1);

                Array array = rs.getArray(2);
                Object[] innerArrayValue1 = (Object[]) array.getArray();
                assertEquals(innerArrayValue1.length, 3);
                assertEquals(innerArrayValue1[0], "1");
                assertEquals(innerArrayValue1[1], "2");
                assertEquals(innerArrayValue1[2], "3");

                Array array2 = rs.getArray(3);
                Object[] innerArrayValue2 = (Object[]) array2.getArray();
                assertEquals(innerArrayValue2.length, 3);
                Integer[] value2 = Arrays.stream(innerArrayValue2).map(Integer.class::cast).toArray(Integer[]::new);
                assertTrue(Arrays.equals(value2, new Integer[]{4, 5, 6}));

            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS nested_test");
        });
    }
}
