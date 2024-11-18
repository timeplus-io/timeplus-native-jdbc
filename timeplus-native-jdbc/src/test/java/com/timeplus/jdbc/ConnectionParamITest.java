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

import com.timeplus.exception.InvalidValueException;
import com.timeplus.settings.TimeplusConfig;
import com.timeplus.settings.SettingKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Locale;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectionParamITest extends AbstractITest {

    @BeforeEach
    public void init() throws SQLException {
        resetDriverManager();
    }

    @Test
    public void connectionPatternTest() {
        String[] jdbcFalse = new String[]{
                "//ck1:",
                "//,ck1",
                "//ck1,ck2,",
                "//ck1:,ck2",
                "//ck1,ck2/"
        };

        for (String jdbc : jdbcFalse) {
            assertFalse(TimeplusJdbcUrlParser.CONNECTION_PATTERN.matcher(jdbc).matches());
        }

        String[] jdbcTrue = new String[]{
                "//ch1?max_rows_to_read=1&connect_timeout=10",
                "//ch1/default?max_rows_to_read=1&connect_timeout=10",
                "//ch1:1234?max_rows_to_read=1&connect_timeout=10",
                "//ch1,ch2?max_rows_to_read=1&connect_timeout=10",
                "//ch1,ch2:1234?max_rows_to_read=1&connect_timeout=10",
                "//ch1:1234,ch2:1234?max_rows_to_read=1&connect_timeout=10",
                "//ch1:1234,ch2:1234/default?max_rows_to_read=1&connect_timeout=10",
                "//ch1:1234,ch2:1234,ch3:2222/default?max_rows_to_read=1&connect_timeout=10",
                "//ch1:1234,ch2,ch3/default?max_rows_to_read=1&connect_timeout=10"
        };

        for (String jdbc : jdbcTrue) {
            TimeplusConfig cfg = TimeplusConfig.Builder.builder()
                    .withJdbcUrl(TimeplusJdbcUrlParser.JDBC_TIMEPLUS_PREFIX + jdbc)
                    .build();

            if (jdbc.contains("?")) {
                assertEquals(cfg.connectTimeout().getSeconds(), 10);
                assertEquals((Long) cfg.settings().get(SettingKey.max_rows_to_read), 1L);
            }

            if (jdbc.contains("/")) {
                assertEquals("default", cfg.database());
            }

            if (jdbc.contains(",")) {
                assertEquals(jdbc.split(",").length, cfg.hosts().size());
            }
        }

        String[] fullJdbc = new String[]{
                "//ch1:9000/default?query_timeout=1&connect_timeout=10&charset=UTF-8&client_name=test&tcp_keep_alive=true",
                "//ch1:9001/default?query_timeout=1&connect_timeout=10&charset=UTF-8&client_name=test&tcp_keep_alive=true",
                "//ch1,ch2:9001/default?query_timeout=1&connect_timeout=10&charset=UTF-8&client_name=test&tcp_keep_alive=false",
                "//ch1:9001,ch2:9002/default?query_timeout=1&connect_timeout=10&charset=UTF-8&client_name=test&tcp_keep_alive=false"
        };

        for (String jdbc : fullJdbc) {
            jdbc = TimeplusJdbcUrlParser.JDBC_TIMEPLUS_PREFIX + jdbc;
            TimeplusConfig cfg = TimeplusConfig.Builder.builder()
                    .withJdbcUrl(jdbc)
                    .build();

            assertEquals(jdbc, cfg.jdbcUrl());
        }

    }


    @Test
    public void successfullyMaxRowsToRead() {
        assertThrows(SQLException.class, () -> {
            try (Connection connection = DriverManager
                    .getConnection(String.format(Locale.ROOT, "jdbc:timeplus://%s:%s?max_rows_to_read=1&connect_timeout=10", TP_HOST, TP_PORT))) {
                withStatement(connection, stmt -> {
                    ResultSet rs = stmt.executeQuery("SELECT array_join([1,2,3,4]) from numbers(100)");
                    int rowsRead = 0;
                    while (rs.next()) {
                        ++rowsRead;
                    }
                    assertEquals(1, rowsRead); // not reached
                });
            }
        });
    }

    @Test
    public void successfullyMaxResultRows() throws Exception {
        try (Connection connection = DriverManager
                .getConnection(String.format(Locale.ROOT, "jdbc:timeplus://%s:%s?max_result_rows=1&connect_timeout=10", TP_HOST, TP_PORT))
        ) {
            withStatement(connection, stmt -> {
                stmt.setMaxRows(400);
                ResultSet rs = stmt.executeQuery("SELECT array_join([1,2,3,4]) from numbers(100)");
                int rowsRead = 0;
                while (rs.next()) {
                    ++rowsRead;
                }
                assertEquals(400, rowsRead);
            });
        }
    }

    @Test
    public void successfullyUrlParser() {
        String url = "jdbc:timeplus://127.0.0.1/system?min_insert_block_size_rows=1000&connect_timeout=50";
        TimeplusConfig config = TimeplusConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));

        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyHostNameOnly() {
        String url = "jdbc:timeplus://my_timeplus_sever_host_name/system?min_insert_block_size_rows=1000&connect_timeout=50";
        TimeplusConfig config = TimeplusConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("my_timeplus_sever_host_name", config.host());
        assertEquals(9000, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyHostNameWithDefaultPort() {
        String url = "jdbc:timeplus://my_timeplus_sever_host_name:9000/system?min_insert_block_size_rows=1000&connect_timeout=50";
        TimeplusConfig config = TimeplusConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("my_timeplus_sever_host_name", config.host());
        assertEquals(9000, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyHostNameWithCustomPort() {
        String url = "jdbc:timeplus://my_timeplus_sever_host_name:1940/system?min_insert_block_size_rows=1000&connect_timeout=50";
        TimeplusConfig config = TimeplusConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("my_timeplus_sever_host_name", config.host());
        assertEquals(1940, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyFailoverHostNameWithCustomPort() {
        String url = "jdbc:timeplus://my_timeplus_sever_host_name1:1940,my_timeplus_sever_host_name2:1941/system?min_insert_block_size_rows=1000&connect_timeout=50";
        TimeplusConfig config = TimeplusConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("my_timeplus_sever_host_name1:1940,my_timeplus_sever_host_name2:1941", config.host());
        assertEquals(2, config.hosts().size());
        assertEquals(9000, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successfullyFailoverHostNameWithDefaultPort() {
        String url = "jdbc:timeplus://my_timeplus_sever_host_name1,my_timeplus_sever_host_name2/system?min_insert_block_size_rows=1000&connect_timeout=50";
        TimeplusConfig config = TimeplusConfig.Builder.builder().withJdbcUrl(url).build();
        assertEquals("my_timeplus_sever_host_name1,my_timeplus_sever_host_name2", config.host());
        assertEquals(2, config.hosts().size());
        assertEquals(9000, config.port());
        assertEquals("system", config.database());
        assertEquals(1000L, config.settings().get(SettingKey.min_insert_block_size_rows));
        assertEquals(Duration.ofSeconds(50), config.connectTimeout());
    }

    @Test
    public void successWrongUrlParser() {
        String url = "jdbc:timeplus://127.0.0. 1/system?min_insert_block_size_rows=1000&connect_timeout=50";
        assertThrows(InvalidValueException.class, () -> TimeplusConfig.Builder.builder().withJdbcUrl(url).build());
    }

    @Test
    public void successWithQueryId() throws Exception {
        String queryId = UUID.randomUUID().toString();

        try (Connection connection = DriverManager
                .getConnection(String.format(Locale.ROOT, "jdbc:timeplus://%s:%s", TP_HOST, TP_PORT))
        ) {
            withStatement(connection, stmt -> {
                stmt.execute("SELECT 1");
                Thread.sleep(12 * 1000); // wait insert into ch log table
                ResultSet rs = stmt.executeQuery(String.format(Locale.ROOT, "SELECT count() FROM system.query_log where query_id='%s'", queryId));

                long rows = 0;

                if (rs.next()) {
                    rows = rs.getLong(1);
                }

                assertTrue(rows == 0);
            });
        }

        try (Connection connection = DriverManager
                .getConnection(String.format(Locale.ROOT, "jdbc:timeplus://%s:%s?query_id=%s", TP_HOST, TP_PORT,
                        queryId))
        ) {
            withStatement(connection, stmt -> {
                stmt.execute("SELECT 1");
                Thread.sleep(12 * 1000); // wait insert into ch log table
                ResultSet rs = stmt.executeQuery(String.format(Locale.ROOT, "SELECT count() FROM system.query_log where query_id='%s'", queryId));

                long rows = 0;

                if (rs.next()) {
                    rows = rs.getLong(1);
                }

                assertTrue(rows > 0);
            });
        }
    }
}
