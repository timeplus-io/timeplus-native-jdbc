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

package com.timeplus.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ResultSetMetadataITest extends AbstractITest {

    @Test
    public void successfullyMetaData() throws Exception {
        withStatement(statement -> {
            statement.executeQuery("DROP STREAM IF EXISTS test");
            statement.executeQuery("CREATE STREAM test(a uint8, b uint64, c fixed_string(3) )ENGINE=Memory");
            statement.executeQuery("INSERT INTO test VALUES (1, 2, '4' )");
            ResultSet rs = statement.executeQuery("SELECT * FROM test");
            ResultSetMetaData metadata = rs.getMetaData();

            assertEquals("test", metadata.getTableName(1));
            assertEquals("default", metadata.getCatalogName(1));

            assertEquals(3, metadata.getPrecision(1));
            assertEquals(19, metadata.getPrecision(2));
            assertEquals(3, metadata.getPrecision(3));
            statement.executeQuery("DROP STREAM IF EXISTS test");
        });
    }

}
