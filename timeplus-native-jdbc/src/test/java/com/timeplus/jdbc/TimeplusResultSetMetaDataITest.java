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

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.*;

class TimeplusResultSetMetaDataITest extends AbstractITest {

    @Test
    void getDriverMinorVersion() throws Exception {
        withNewConnection(connection -> {
            TimeplusResultSet rs = TimeplusResultSetBuilder
                                         .builder(8, ((TimeplusConnection) connection).serverContext())
                                         .cfg(((TimeplusConnection) connection).cfg())
                                         .columnNames("a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8")
                                         .columnTypes("string", "uint32", "int64", "float32", "float64", "decimal(76, 26)",
                                                 "nullable(int64)", "nullable(uint64)")
                                         .build();

            assertEquals("a1", rs.getMetaData().getColumnName(1));

            assertFalse(rs.getMetaData().isSigned(2));
            assertTrue(rs.getMetaData().isSigned(3));
            assertTrue(rs.getMetaData().isSigned(4));
            assertTrue(rs.getMetaData().isSigned(5));
            assertTrue(rs.getMetaData().isSigned(7));
            assertFalse(rs.getMetaData().isSigned(8));

            assertEquals(8, rs.getMetaData().getPrecision(4));
            assertEquals(17, rs.getMetaData().getPrecision(5));
            assertEquals(76, rs.getMetaData().getPrecision(6));

            assertEquals(8, rs.getMetaData().getScale(4));
            assertEquals(17, rs.getMetaData().getScale(5));
            assertEquals(26, rs.getMetaData().getScale(6));

            assertEquals("a4", rs.getMetaData().getColumnName(4));
            assertEquals("float64", rs.getMetaData().getColumnTypeName(5));
            assertEquals(Types.DECIMAL, rs.getMetaData().getColumnType(6));

            assertFalse(rs.next());
        });
    }
}
