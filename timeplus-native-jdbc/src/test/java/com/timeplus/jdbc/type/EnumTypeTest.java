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

public class EnumTypeTest extends AbstractITest implements BytesHelper {

    @Test
    public void testEnum8Type() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS enum8_test");
            statement.execute("CREATE STREAM IF NOT EXISTS enum8_test (value enum8('test', 'test2')) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO enum8_test values(?), (?);")) {
                for (int i = 0; i < rowCnt/2; i++) {
                    pstmt.setString(1, "test");
                    pstmt.addBatch();
                }
                for (int i = 0; i < rowCnt/2; i++) {
                    pstmt.setString(1, "test2");
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM enum8_test;");
            int size = 0;
            while (rs.next()&&size++<rowCnt/2) {
                String value = rs.getString(1);
                assertEquals(value, "test");
            }
            while (rs.next()) {
                size++;
                String value = rs.getString(1);
                assertEquals(value, "test2");
            }
            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS enum8_test");
        });

    }

    // test for fixed_string
    @Test
    public void testEnum16Type() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS enum16_test");
            statement.execute("CREATE STREAM IF NOT EXISTS enum16_test (value enum8('test', 'test2')) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO enum16_test values(?), (?);")) {
                for (int i = 0; i < rowCnt/2; i++) {
                    pstmt.setString(1, "test");
                    pstmt.addBatch();
                }
                for (int i = 0; i < rowCnt/2; i++) {
                    pstmt.setString(1, "test2");
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM enum16_test;");
            int size = 0;
            while (rs.next()&&size++<rowCnt/2) {
                String value = rs.getString(1);
                assertEquals(value, "test");
            }
            while (rs.next()) {
                size++;
                String value = rs.getString(1);
                assertEquals(value, "test2");
            }
            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS enum16_test");
        });
    }
}

