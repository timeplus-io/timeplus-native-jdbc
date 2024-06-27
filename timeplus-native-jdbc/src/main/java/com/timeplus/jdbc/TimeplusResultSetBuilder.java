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

package com.github.timeplus.jdbc;

import com.github.timeplus.client.NativeContext;
import com.github.timeplus.misc.Validate;
import com.github.timeplus.settings.TimeplusConfig;
import com.github.timeplus.settings.TimeplusDefines;
import com.github.timeplus.stream.QueryResult;
import com.github.timeplus.stream.QueryResultBuilder;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TimeplusResultSetBuilder {

    private final QueryResultBuilder queryResultBuilder;

    private TimeplusConfig cfg;
    private String db = TimeplusDefines.DEFAULT_DATABASE;
    private String table = "unknown";

    public static TimeplusResultSetBuilder builder(int columnsNum, NativeContext.ServerContext serverContext) {
        return new TimeplusResultSetBuilder(QueryResultBuilder.builder(columnsNum, serverContext));
    }

    private TimeplusResultSetBuilder(QueryResultBuilder queryResultBuilder) {
        this.queryResultBuilder = queryResultBuilder;
    }

    public TimeplusResultSetBuilder cfg(TimeplusConfig cfg) {
        this.cfg = cfg;
        return this;
    }

    public TimeplusResultSetBuilder db(String db) {
        this.db = db;
        return this;
    }

    public TimeplusResultSetBuilder table(String table) {
        this.table = table;
        return this;
    }

    public TimeplusResultSetBuilder columnNames(String... names) {
        return columnNames(Arrays.asList(names));
    }

    public TimeplusResultSetBuilder columnNames(List<String> names) {
        this.queryResultBuilder.columnNames(names);
        return this;
    }

    public TimeplusResultSetBuilder columnTypes(String... types) throws SQLException {
        return columnTypes(Arrays.asList(types));
    }

    public TimeplusResultSetBuilder columnTypes(List<String> types) throws SQLException {
        this.queryResultBuilder.columnTypes(types);
        return this;
    }

    public TimeplusResultSetBuilder addRow(Object... row) {
        return addRow(Arrays.asList(row));
    }

    public TimeplusResultSetBuilder addRow(List<?> row) {
        this.queryResultBuilder.addRow(row);
        return this;
    }

    public TimeplusResultSet build() throws SQLException {
        Validate.ensure(cfg != null);
        QueryResult queryResult = this.queryResultBuilder.build();
        return new TimeplusResultSet(null, cfg, db, table, queryResult.header(), queryResult.data());
    }
}
