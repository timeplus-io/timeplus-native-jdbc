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

 package com.github.timeplus.jdbc.type;

 import com.github.timeplus.jdbc.AbstractITest;
 import com.github.timeplus.misc.BytesHelper;
 import org.junit.jupiter.api.Test;
 
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 
 import static org.junit.jupiter.api.Assertions.assertEquals;
 
 public class StringTypeTest extends AbstractITest implements BytesHelper {
    @Test
    public void testStringType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS string_test");
            statement.execute("CREATE STREAM IF NOT EXISTS string_test (value string, nullableValue nullable(string)) Engine=Memory()");
 
            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO string_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setString(1, "test");
                    pstmt.setString(2, "test2");
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
 
            ResultSet rs = statement.executeQuery("SELECT * FROM string_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                String value = rs.getString(1);
                assertEquals(value, "test");
                String nullableValue = rs.getString(2);
                assertEquals(nullableValue, "test2");
            }
 
            assertEquals(size, rowCnt);
 
            statement.execute("DROP STREAM IF EXISTS string_test");
        });
 
    }

    // test for fixed_string
    @Test
    public void testFixedStringType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS fixed_string_test");
            statement.execute("CREATE STREAM IF NOT EXISTS fixed_string_test (value fixed_string(10), nullableValue nullable(fixed_string(10))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO fixed_string_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setString(1, "abcdefghij");
                    pstmt.setString(2, "klmnopqrst");
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM fixed_string_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                String value = rs.getString(1);
                assertEquals(value, "abcdefghij");
                String nullableValue = rs.getString(2);
                assertEquals(nullableValue, "klmnopqrst");
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS fixed_string_test");
        });
    }
 }

