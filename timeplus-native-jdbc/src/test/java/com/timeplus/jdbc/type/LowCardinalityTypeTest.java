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
import com.timeplus.jdbc.TimeplusStruct;
import com.timeplus.misc.BytesHelper;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

//Refer to [[https://github.com/timeplus-io/timeplus-native-jdbc/issues/442]] for more details.
public class LowCardinalityTypeTest extends AbstractITest implements BytesHelper {
    @Test
    public void testAllLowCardinalityTypes() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS low_cardinality_test");

            StringBuilder createTableSQL = new StringBuilder();
            createTableSQL.append("CREATE STREAM IF NOT EXISTS low_cardinality_test (")
                    .append("value_string low_cardinality(string), ")
                    .append("fixed_string low_cardinality(fixed_string(10)), ")
                    .append("date_value low_cardinality(date), ")
                    .append("datetime_value low_cardinality(datetime), ")
                    .append("number_value low_cardinality(int32)) Engine=Memory()");

            statement.execute(createTableSQL.toString());

            String sql = "INSERT INTO low_cardinality_test " +
                    "(value_string, fixed_string, date_value, datetime_value, number_value) values(?, ?, ?, ?, ?);";

            Integer rowCnt = 100;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(sql)) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setString(1, "test");
                    pstmt.setString(2, "abcdefgj");
                    pstmt.setDate(3, new java.sql.Date(System.currentTimeMillis()));
                    pstmt.setTimestamp(4, new java.sql.Timestamp(System.currentTimeMillis()));
                    pstmt.setInt(5, i);

                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            DatabaseMetaData metaData = statement.getConnection().getMetaData();
            ResultSet columns = metaData.getColumns(null, "default", "low_cardinality_test", "%");
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String columnType = columns.getString("TYPE_NAME");
                switch (columnName) {
                    case "value_string":
                        assertEquals(columnType, "low_cardinality(string)");
                        break;
                    case "fixed_string":
                        assertEquals(columnType, "low_cardinality(fixed_string(10))");
                        break;
                    case "date_value":
                        assertEquals(columnType, "low_cardinality(date)");
                        break;
                    case "datetime_value":
                        assertEquals(columnType, "low_cardinality(datetime)");
                        break;
                    case "number_value":
                        assertEquals(columnType, "low_cardinality(int32)");
                        break;
                }
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM low_cardinality_test;");
            int size = 0;
            while (rs.next()) {
                String valueStr = rs.getString("value_string");
                String fixedStr = rs.getString("fixed_string");
                Date dateValue = rs.getDate("date_value");
                Timestamp datetimeValue = rs.getTimestamp("datetime_value");
                Integer numberValue = rs.getInt("number_value");

                assertEquals("test", valueStr);
                assertEquals("abcdefgj\0\0", fixedStr);
                assertNotNull(dateValue);
                assertNotNull(datetimeValue);
                assertTrue(numberValue >= 0 && numberValue < 300);

                size++;
            }
            assertEquals(rowCnt, size);

            statement.execute("DROP STREAM IF EXISTS low_cardinality_test");
        }, "allow_suspicious_low_cardinality_types", "1");
    }

    @Test
    public void testNullLowCardinalityTypes() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS low_cardinality_test");

            StringBuilder createTableSQL = new StringBuilder();
            createTableSQL.append("CREATE STREAM IF NOT EXISTS low_cardinality_test (")
                    .append("value_string low_cardinality(nullable(string)), ")
                    .append("fixed_string low_cardinality(nullable(fixed_string(10))), ")
                    .append("date_value low_cardinality(nullable(date)), ")
                    .append("datetime_value low_cardinality(nullable(datetime)), ")
                    .append("number_value low_cardinality(nullable(int32))) Engine=Memory()");

            statement.execute(createTableSQL.toString());

            String sql = "INSERT INTO low_cardinality_test " +
                    "(value_string, fixed_string, date_value, datetime_value, number_value) values(?, ?, ?, ?, ?);";
            
            Integer rowCnt = 100;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(sql)) {
                for (int i = 0; i < rowCnt/2; i++) {
                    pstmt.setString(1, "test");
                    pstmt.setString(2, "abcdefgj");
                    pstmt.setDate(3, new java.sql.Date(System.currentTimeMillis()));
                    pstmt.setTimestamp(4, new java.sql.Timestamp(System.currentTimeMillis()));
                    pstmt.setInt(5, i);

                    pstmt.addBatch();
                }
                for (int i = 0; i < rowCnt/2; i++) {
                    pstmt.setNull(1, 1);
                    pstmt.setNull(2, 1);
                    pstmt.setNull(3, 1);
                    pstmt.setNull(4, 1);
                    pstmt.setNull(5, 1);

                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            DatabaseMetaData metaData = statement.getConnection().getMetaData();
            ResultSet columns = metaData.getColumns(null, "default", "low_cardinality_test", "%");
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String columnType = columns.getString("TYPE_NAME");
                switch (columnName) {
                    case "value_string":
                        assertEquals(columnType, "low_cardinality(nullable(string))");
                        break;
                    case "fixed_string":
                        assertEquals(columnType, "low_cardinality(nullable(fixed_string(10)))");
                        break;
                    case "date_value":
                        assertEquals(columnType, "low_cardinality(nullable(date))");
                        break;
                    case "datetime_value":
                        assertEquals(columnType, "low_cardinality(nullable(datetime))");
                        break;
                    case "number_value":
                        assertEquals(columnType, "low_cardinality(nullable(int32))");
                        break;
                }
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM low_cardinality_test;");
            int size = 0;
            while (rs.next()) {
                String valueStr = rs.getString("value_string");
                String fixedStr = rs.getString("fixed_string");
                Date dateValue = rs.getDate("date_value");
                Timestamp datetimeValue = rs.getTimestamp("datetime_value");
                Integer numberValue = (Integer) rs.getObject("number_value");

                if (size < rowCnt / 2) {
                    assertEquals("test", valueStr);
                    assertEquals("abcdefgj\0\0", fixedStr);
                    assertNotNull(dateValue);
                    assertNotNull(datetimeValue);
                    assertTrue(numberValue >= 0 && numberValue < 300);
                }
                else {
                    assertNull(valueStr);
                    assertNull(fixedStr);
                    assertNull(dateValue);
                    assertNull(datetimeValue);
                    assertNull(numberValue);
                }
                size++;
            }
            assertEquals(rowCnt, size);

            statement.execute("DROP STREAM IF EXISTS low_cardinality_test");
        }, "allow_suspicious_low_cardinality_types", "1");
    }

    @Test
    public void testMapNestedType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS nested_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS nested_test (value map(low_cardinality(string), low_cardinality(int32))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO nested_test (value) values(?);")) {

                Map<String, Integer> map = new HashMap<>();
                map.put("key", 1);
                map.put("key2", 2);
                map.put("key3", 3);
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setObject(1, map);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM nested_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Map<String, Integer> value = (Map<String, Integer>) rs.getObject(1);
                assertEquals(value.get("key"), 1);
                assertEquals(value.get("key2"), 2);
                assertEquals(value.get("key3"), 3);

            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS nested_test");
        });
    }

    @Test
    public void testTupleNestedType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS nested_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS nested_test (value tuple(low_cardinality(string), low_cardinality(int32))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO nested_test (value) values(?);")) {

                Object[] tupleValue = new Object[]{"test", 1};
                TimeplusStruct tuple = new TimeplusStruct("tuple", tupleValue);
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setObject(1, tuple);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM nested_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object obj1 = rs.getObject(1);
                TimeplusStruct tuple = (TimeplusStruct) obj1;
                Object[] tupleValue = (Object[]) tuple.getAttributes();
                assertEquals(tupleValue[0], "test");
                assertEquals(tupleValue[1], 1);

            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS nested_test");
        });
    }
}
