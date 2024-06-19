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
 
 public class DateTypeTest extends AbstractITest implements BytesHelper {
 
    @Test
    public void testDateType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS date_test");
            statement.execute("CREATE STREAM IF NOT EXISTS date_test (value date, nullableValue nullable(date)) Engine=Memory()");
 
            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO date_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setDate(1, java.sql.Date.valueOf("2021-01-01"));
                    pstmt.setDate(2, java.sql.Date.valueOf("2021-01-02"));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
 
            ResultSet rs = statement.executeQuery("SELECT * FROM date_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                java.sql.Date value = rs.getDate(1);
                assertEquals(value, java.sql.Date.valueOf("2021-01-01"));
                java.sql.Date nullableValue = rs.getDate(2);
                assertEquals(nullableValue, java.sql.Date.valueOf("2021-01-02"));
            }
 
            assertEquals(size, rowCnt);
 
            statement.execute("DROP STREAM IF EXISTS date_test");
        });
 
    }

    // test for datetime
    @Test
    public void testDateTimeType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS datetime_test");
            statement.execute("CREATE STREAM IF NOT EXISTS datetime_test (value datetime, nullableValue nullable(datetime)) Engine=Memory()");
 
            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO datetime_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setTimestamp(1, java.sql.Timestamp.valueOf("2021-01-01 00:00:00"));
                    pstmt.setTimestamp(2, java.sql.Timestamp.valueOf("2021-01-02 00:00:00"));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
 
            ResultSet rs = statement.executeQuery("SELECT * FROM datetime_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                java.sql.Timestamp value = rs.getTimestamp(1);
                assertEquals(value, java.sql.Timestamp.valueOf("2021-01-01 00:00:00"));
                java.sql.Timestamp nullableValue = rs.getTimestamp(2);
                assertEquals(nullableValue, java.sql.Timestamp.valueOf("2021-01-02 00:00:00"));
            }
 
            assertEquals(size, rowCnt);
 
            statement.execute("DROP STREAM IF EXISTS datetime_test");
        });
 
    }

    // test for datetime64
    @Test
    public void testDateTime64Type() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS datetime64_test");
            statement.execute("CREATE STREAM IF NOT EXISTS datetime64_test (value datetime64(3), nullableValue nullable(datetime64(3))) Engine=Memory()");
 
            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO datetime64_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setTimestamp(1, java.sql.Timestamp.valueOf("2021-01-01 00:00:00.000"));
                    pstmt.setTimestamp(2, java.sql.Timestamp.valueOf("2021-01-02 00:00:00.000"));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
 
            ResultSet rs = statement.executeQuery("SELECT * FROM datetime64_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                java.sql.Timestamp value = rs.getTimestamp(1);
                assertEquals(value, java.sql.Timestamp.valueOf("2021-01-01 00:00:00.000"));
                java.sql.Timestamp nullableValue = rs.getTimestamp(2);
                assertEquals(nullableValue, java.sql.Timestamp.valueOf("2021-01-02 00:00:00.000"));
            }
 
            assertEquals(size, rowCnt);
 
            statement.execute("DROP STREAM IF EXISTS datetime64_test");
        });
 
    }

 
 }
