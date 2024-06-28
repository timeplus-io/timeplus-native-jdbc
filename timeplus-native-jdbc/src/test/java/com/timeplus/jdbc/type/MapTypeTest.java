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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapTypeTest extends AbstractITest implements BytesHelper {

    // test for map(string, int)
    @Test
    public void testMapType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS map_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS map_test (value map(string, int)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO map_test (value) values(?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    
                    Map<String, Integer> map = Map.of("key", 1);

                    pstmt.setObject(1, map);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM map_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                // check the value
                Map<String, Integer> value = (Map<String, Integer>) rs.getObject(1);
                assertEquals(value.get("key"), 1);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS map_test");
        });

    }
}
