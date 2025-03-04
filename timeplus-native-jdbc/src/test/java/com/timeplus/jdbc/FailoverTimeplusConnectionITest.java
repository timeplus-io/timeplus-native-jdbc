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

import com.timeplus.log.Logger;
import com.timeplus.log.LoggerFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.timeplus.TimeplusContainer;
import org.testcontainers.junit.jupiter.Container;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FailoverTimeplusConnectionITest extends AbstractITest {
    private static final Logger LOG = LoggerFactory.getLogger(FailoverTimeplusConnectionITest.class);
    private static final Integer NATIVE_PORT = 8463;
    protected static String HA_HOST;
    protected static int HA_PORT;

    @Container
    public static TimeplusContainer containerHA = new TimeplusContainer(AbstractITest.TIMEPLUS_IMAGE)
            .withEnv("TIMEPLUS_USER", TIMEPLUS_USER)
            .withEnv("TIMEPLUS_PASSWORD", TIMEPLUS_PASSWORD)
            .withEnv("TIMEPLUS_DB", TIMEPLUS_DB);


    @BeforeEach
    public void reset() throws SQLException {
        resetDriverManager();
        container.start();
        containerHA.start();

        TP_PORT = container.getMappedPort(NATIVE_PORT);
        HA_HOST = containerHA.getHost();
        HA_PORT = containerHA.getMappedPort(NATIVE_PORT);
        LOG.info("Port1 {}, Port2 {}", TP_PORT, HA_PORT);
    }

    @Test
    public void testTimeplusDownBeforeConnect() throws Exception {
        String haHost = String.format(Locale.ROOT, "%s:%s,%s:%s", TP_HOST, TP_PORT, HA_HOST, HA_PORT);

        container.stop();
        try (Connection connection = DriverManager
                .getConnection(String.format(Locale.ROOT, "jdbc:timeplus://%s/default", haHost))
        ) {
            withStatement(connection, stmt -> {
                ResultSet rs = stmt.executeQuery("select count() from system.tables");

                if (rs.next()) {
                    assertTrue(rs.getLong(1) == 0);
                }
            });
        }
    }

    @Test
    public void testTimeplusDownBeforeStatement() throws Exception {
        String haHost = String.format(Locale.ROOT, "%s:%s,%s:%s", TP_HOST, TP_PORT, HA_HOST, HA_PORT);

        try (Connection connection = DriverManager
                .getConnection(String.format(Locale.ROOT, "jdbc:timeplus://%s/default", haHost))
        ) {
            container.stop();
            withStatement(connection, stmt -> {
                ResultSet rs = stmt.executeQuery("select count() from system.tables");

                if (rs.next()) {
                    assertTrue(rs.getLong(1) == 0);
                }
            });
        }
    }

    @Test
    public void testTimeplusDownBeforePrepareStatement() throws Exception {
        String haHost = String.format(Locale.ROOT, "%s:%s,%s:%s", TP_HOST, TP_PORT, HA_HOST, HA_PORT);

        try (Connection connection = DriverManager
                .getConnection(String.format(Locale.ROOT, "jdbc:timeplus://%s/default", haHost))
        ) {
            container.stop();
            withPreparedStatement(connection, "select count() from system.tables", stmt -> {
                ResultSet rs = stmt.executeQuery();

                if (rs.next()) {
                    assertTrue(rs.getLong(1) == 0);
                }
            });
        }
    }

    @Test
    public void testTimeplusDownBeforeExecute() throws Exception {
        String haHost = String.format(Locale.ROOT, "%s:%s,%s:%s", TP_HOST, TP_PORT, HA_HOST, HA_PORT);

        try (Connection connection = DriverManager
                .getConnection(String.format(Locale.ROOT, "jdbc:timeplus://%s/default", haHost))
        ) {
            withStatement(connection, stmt -> {
                container.stop();
                ResultSet rs = stmt.executeQuery("select count() from system.tables");

                if (rs.next()) {
                    assertTrue(rs.getLong(1) == 0);
                }
            });
        }
    }

    @Test
    public void testTimeplusDownBeforeAndAfterConnect() {
        String haHost = String.format(Locale.ROOT, "%s:%s,%s:%s", TP_HOST, TP_PORT, HA_HOST, HA_PORT);

        Exception ex = null;
        container.stop();
        try (Connection connection = DriverManager
                .getConnection(String.format(Locale.ROOT, "jdbc:timeplus://%s/default?query_id=xxx", haHost))
        ) {
            containerHA.stop();
            withStatement(connection, stmt -> {
                ResultSet rs = stmt.executeQuery("select count() from system.tables");

                if (rs.next()) {
                    assertTrue(rs.getLong(1) > 0);
                }
            });
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(ex);
    }

    @Test
    public void testTimeplusAllDownBeforeConnect() throws Exception {
        String haHost = String.format(Locale.ROOT, "%s:%s,%s", TP_HOST, TP_PORT, HA_HOST);

        Exception ex = null;
        container.stop();
        containerHA.stop();
        try (Connection connection = DriverManager
                .getConnection(String.format(Locale.ROOT, "jdbc:timeplus://%s/default", haHost))
        ) {
            withStatement(connection, stmt -> {

                ResultSet rs = stmt.executeQuery("select count() from system.tables");

                if (rs.next()) {
                    assertTrue(rs.getLong(1) > 0);
                }
            });
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(ex);
    }
}
