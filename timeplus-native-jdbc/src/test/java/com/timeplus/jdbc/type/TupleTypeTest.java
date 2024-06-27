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
import com.github.timeplus.jdbc.TimeplusStruct;
import com.github.timeplus.misc.BytesHelper;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TupleTypeTest extends AbstractITest implements BytesHelper {

    // test for tuple(int, string)
    @Test
    public void testTupleType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS tuple_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS tuple_test (value tuple(int, string)) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO tuple_test (value) values(?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    Object[] tupleValue = new Object[]{1, "test"};
                    TimeplusStruct tuple = new TimeplusStruct("tuple", tupleValue);
                    // convert to TimeplusStruct

                    pstmt.setObject(1, tuple);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM tuple_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object obj1 = rs.getObject(1);
                TimeplusStruct tuple = (TimeplusStruct) obj1;
                Object[] tupleValue = (Object[]) tuple.getAttributes();
                assertEquals(tupleValue[0], 1);
                assertEquals(tupleValue[1], "test");
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS tuple_test");
        });

    }

}
