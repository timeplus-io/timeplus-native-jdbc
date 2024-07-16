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

import com.timeplus.misc.StrUtil;
import com.timeplus.misc.SystemUtil;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.timeplus.TimeplusContainer;
import org.testcontainers.utility.MountableFile;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.*;
import java.time.ZoneId;
import java.util.Enumeration;

@Testcontainers
public abstract class AbstractITest implements Serializable {

    protected static final ZoneId CLIENT_TZ = ZoneId.systemDefault();
    protected static final ZoneId SERVER_TZ = ZoneId.of("UTC");
    protected static final String DRIVER_CLASS_NAME = "com.timeplus.jdbc.TimeplusDriver";

    public static final String TIMEPLUS_IMAGE = System.getProperty("TIMEPLUS_IMAGE", "timeplus/timeplusd:2.3.3");
    // public static DockerImageName proton_image = DockerImageName.parse(CLICKHOUSE_IMAGE).asCompatibleSubstituteFor("clickhouse/clickhouse-server");

    protected static final String TIMEPLUS_USER = SystemUtil.loadProp("CLICKHOUSE_USER", "");
    protected static final String TIMEPLUS_PASSWORD = SystemUtil.loadProp("CLICKHOUSE_PASSWORD", "");
    protected static final String TIMEPLUS_DB = SystemUtil.loadProp("CLICKHOUSE_DB", "");

    protected static final int TIMEPLUS_HTTP_PORT = 3218;
    protected static final int TIMEPLUS_HTTPS_PORT = 3218;
    protected static final int TIMEPLUS_NATIVE_PORT = 8463;
    protected static final int TIMEPLUS_NATIVE_SECURE_PORT = 8463;

    @Container
    public static TimeplusContainer container = new TimeplusContainer(TIMEPLUS_IMAGE)
            .withEnv("CLICKHOUSE_USER", TIMEPLUS_USER)
            .withEnv("CLICKHOUSE_PASSWORD", TIMEPLUS_PASSWORD)
            .withEnv("CLICKHOUSE_DB", TIMEPLUS_DB)
            .withExposedPorts(TIMEPLUS_HTTP_PORT,
                    TIMEPLUS_HTTPS_PORT,
                    TIMEPLUS_NATIVE_PORT,
                    TIMEPLUS_NATIVE_SECURE_PORT)
            // .withCopyFileToContainer(MountableFile.forClasspathResource("timeplus/config/config.yaml"),
            //         "/etc/timeplusd-server/config.yaml")
            .withCopyFileToContainer(MountableFile.forClasspathResource("timeplus/config/users.yaml"),
                    "/etc/timeplusd-server/users.yaml")
            .withCopyFileToContainer(MountableFile.forClasspathResource("timeplus/server.key"),
                    "/etc/timeplusd-server/server.key")
            .withCopyFileToContainer(MountableFile.forClasspathResource("timeplus/server.crt"),
                    "/etc/timeplusd-server/server.crt");

    protected static String TP_HOST;
    protected static int TP_PORT;

    @BeforeAll
    public static void extractContainerInfo() {
        TP_HOST = container.getHost();
        TP_PORT = container.getMappedPort(TIMEPLUS_NATIVE_PORT);
    }

    /**
     * just for compatible with scala
     */
    protected String getJdbcUrl() {
        return getJdbcUrl("");
    }

    protected String getJdbcUrl(Object... params) {
        StringBuilder settingsStringBuilder = new StringBuilder();
        for (int i = 0; i + 1 < params.length; i+=2) {
            settingsStringBuilder.append(i == 0 ? "?" : "&");
            settingsStringBuilder.append(params[i]).append("=").append(params[i + 1]);
        }

        StringBuilder mainStringBuilder = new StringBuilder();
        int port = 0;
        if (settingsStringBuilder.indexOf("ssl=true") == -1) {
            port = container.getMappedPort(TIMEPLUS_NATIVE_PORT);
        } else {
            port = container.getMappedPort(TIMEPLUS_NATIVE_SECURE_PORT);
        }

        mainStringBuilder.append("jdbc:timeplus://").append(container.getHost()).append(":").append(port);
        if (StrUtil.isNotEmpty(TIMEPLUS_DB)) {
            mainStringBuilder.append("/").append(container.getDatabaseName());
        }

        // Add settings
        mainStringBuilder.append(settingsStringBuilder);

        // Add user
        mainStringBuilder.append(params.length < 2 ? "?" : "&");
        mainStringBuilder.append("user=").append(container.getUsername());

        // Add password
        // ignore blank password
        if (!StrUtil.isBlank(TIMEPLUS_PASSWORD)) {
            mainStringBuilder.append("&password=").append(container.getPassword());
        }

        return mainStringBuilder.toString();
    }

    // this method should be synchronized
    synchronized protected void resetDriverManager() throws SQLException {
        // remove all registered jdbc drivers
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            DriverManager.deregisterDriver(drivers.nextElement());
        }
        DriverManager.registerDriver(new TimeplusDriver());
    }

    protected void withNewConnection(WithConnection withConnection, Object... args) throws Exception {
        resetDriverManager();

        String connectionStr = getJdbcUrl(args);
        try (Connection connection = DriverManager.getConnection(connectionStr)) {
            withConnection.apply(connection);
        }
    }

    protected void withNewConnection(DataSource ds, WithConnection withConnection) throws Exception {
        try (Connection connection = ds.getConnection()) {
            withConnection.apply(connection);
        }
    }

    protected void withStatement(Connection connection, WithStatement withStatement) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            withStatement.apply(stmt);
        }
    }

    protected void withStatement(WithStatement withStatement, Object... args) throws Exception {
        withNewConnection(connection -> withStatement(connection, withStatement), args);
    }

    protected void withPreparedStatement(Connection connection,
                                         String sql,
                                         WithPreparedStatement withPreparedStatement) throws Exception {
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            withPreparedStatement.apply(pstmt);
        }
    }

    protected void withPreparedStatement(String sql,
                                         WithPreparedStatement withPreparedStatement,
                                         Object... args) throws Exception {
        withNewConnection(connection -> withPreparedStatement(connection, sql, withPreparedStatement), args);
    }

    @FunctionalInterface
    public interface WithConnection {
        void apply(Connection connection) throws Exception;
    }

    @FunctionalInterface
    public interface WithStatement {
        void apply(Statement stmt) throws Exception;
    }

    @FunctionalInterface
    public interface WithPreparedStatement {
        void apply(PreparedStatement pstmt) throws Exception;
    }
}
