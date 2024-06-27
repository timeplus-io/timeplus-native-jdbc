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
 
 import static org.junit.jupiter.api.Assertions.assertEquals;
 
 public class FloatTypeTest extends AbstractITest implements BytesHelper {
 
    // test for float32
    @Test
    public void testFloatType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS float_test");
            statement.execute("CREATE STREAM IF NOT EXISTS float_test (value float32, nullableValue nullable(float32)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO float_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setFloat(1, 1.1f);
                    pstmt.setFloat(2, 2.2f);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM float_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Float value = rs.getFloat(1);
                assertEquals(value, 1.1f);
                Float nullableValue = rs.getFloat(2);
                assertEquals(nullableValue, 2.2f);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS float_test");
        });

    }

    // test for float64
    @Test
    public void testDoubleType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS double_test");
            statement.execute("CREATE STREAM IF NOT EXISTS double_test (value float64, nullableValue nullable(float64)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO double_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setDouble(1, 1.1);
                    pstmt.setDouble(2, 2.2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM double_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Double value = rs.getDouble(1);
                assertEquals(value, 1.1);
                Double nullableValue = rs.getDouble(2);
                assertEquals(nullableValue, 2.2);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS double_test");
        });

    }

 
 }

