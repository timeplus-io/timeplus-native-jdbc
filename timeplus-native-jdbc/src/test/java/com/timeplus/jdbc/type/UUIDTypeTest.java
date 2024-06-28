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
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
 
public class UUIDTypeTest extends AbstractITest implements BytesHelper {
    // test for UUID type, create a stream with UUID type and insert values into it and check value is same as inserted
    @Test
    public void testUUIDType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS uuid_test");
            statement.execute("CREATE STREAM IF NOT EXISTS uuid_test (value uuid, nullableValue nullable(uuid)) Engine=Memory()");

            Integer rowCnt = 300;
            List<java.util.UUID> insertedValues = new ArrayList<>();
            List<java.util.UUID> insertedNullableValues = new ArrayList<>();
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO uuid_test (value, nullableValue) values(?, ?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    java.util.UUID uuid1 = java.util.UUID.randomUUID();
                    java.util.UUID uuid2 = java.util.UUID.randomUUID();
                    insertedValues.add(uuid1);
                    insertedNullableValues.add(uuid2);
                    pstmt.setObject(1, uuid1);
                    pstmt.setObject(2, uuid2);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM uuid_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                java.util.UUID value = (java.util.UUID) rs.getObject(1);
                java.util.UUID nullableValue = (java.util.UUID) rs.getObject(2);
                assertEquals(value, insertedValues.get(size - 1));
                assertEquals(nullableValue, insertedNullableValues.get(size - 1));
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS uuid_test");
        });
    }
}

