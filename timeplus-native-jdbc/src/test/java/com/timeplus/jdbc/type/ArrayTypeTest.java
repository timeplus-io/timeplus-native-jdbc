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
 import java.util.Arrays;

 
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 
 import static org.junit.jupiter.api.Assertions.assertEquals;
 import static org.junit.jupiter.api.Assertions.assertTrue;
 
 public class ArrayTypeTest extends AbstractITest implements BytesHelper {
 
    @Test
    public void testArrayType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS array_test");
            statement.execute("CREATE STREAM IF NOT EXISTS array_test (value array(int)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO array_test (value) values(?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setArray(1, statement.getConnection().createArrayOf("int", new Integer[]{1, 2, 3}));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM array_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object[] objArray = (Object[]) rs.getArray(1).getArray();
                Integer[] value = Arrays.stream(objArray).map(Integer.class::cast).toArray(Integer[]::new);
                assertTrue(Arrays.equals(value, new Integer[]{1, 2, 3}));
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS array_test");
        });

    }
 
 }
