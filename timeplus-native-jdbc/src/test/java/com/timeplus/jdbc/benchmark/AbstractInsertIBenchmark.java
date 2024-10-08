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

package com.timeplus.jdbc.benchmark;

import org.openjdk.jmh.annotations.*;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class AbstractInsertIBenchmark extends AbstractIBenchmark {

    @Param({"20", "50"})
    protected int columnNum = 20;

    @Param({"200000", "500000"})
    protected int batchSize = 200000;

    AtomicInteger tableMaxId = new AtomicInteger();

    // DROP STREAM, CREATE STREAM
    protected void wideColumnPrepare(Connection connection, String columnType) throws Exception {
        int tableId = tableMaxId.incrementAndGet();
        String testTable = "test_" + tableId;
        withStatement(connection, stmt -> {
            stmt.executeQuery("DROP STREAM IF EXISTS " + testTable);
            StringBuilder createSQL = new StringBuilder("CREATE STREAM " + testTable + " (");
            for (int i = 0; i < columnNum; i++) {
                createSQL.append("col_").append(i).append(" ").append(columnType);
                if (i + 1 != columnNum) {
                    createSQL.append(",\n");
                }
            }
            stmt.executeQuery(createSQL + ")Engine = Log");
            stmt.close();
        });
    }

    protected void wideColumnAfter(Connection connection) throws Exception {
        withStatement(connection, stmt -> stmt.executeQuery("DROP STREAM " + getTableName()));
    }

    protected String getTableName() {
        return "test_" + tableMaxId.get();
    }
}
