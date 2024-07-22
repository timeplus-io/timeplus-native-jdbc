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

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

//Refer to [[https://github.com/timeplus-io/timeplus-native-jdbc/issues/442]] for more details.
public class LowCardinalityTypeTest extends AbstractITest implements BytesHelper {
    @Test
    public void testAllLowCardinalityTypes() throws Exception {
    // FIXME
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

            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(sql)) {
                for (int i = 0; i < 1; i++) {
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
                Integer numberValue = (Integer) rs.getObject("number_value");

                assertEquals("test", valueStr);
                assertEquals("abcdefgj\0\0", fixedStr);
                assertNotNull(dateValue);
                assertNotNull(datetimeValue);
                assertTrue(numberValue >= 0 && numberValue < 300);

                size++;
            }
            assertEquals(1, size);

            statement.execute("DROP STREAM IF EXISTS low_cardinality_test");
        }, "allow_suspicious_low_cardinality_types", "1");
    }
}
