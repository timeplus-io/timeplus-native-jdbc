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

import com.timeplus.jdbc.AbstractITest;
import com.timeplus.jdbc.TimeplusDriver;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testcontainers.clickhouse.ClickHouseContainer;

import java.sql.*;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class AbstractIBenchmark {

    /*
    * HTTP API Port for http requests. used by JDBC, ODBC and web interfaces.
    * */
    protected static final int TIMEPLUS_HTTP_PORT = 8123;
    protected static final int TIMEPLUS_NATIVE_PORT = 8463;

    public static final ClickHouseContainer container;

    static {
        container = new ClickHouseContainer(AbstractITest.TIMEPLUS_IMAGE);
        container.start();
    }

    private final Driver timeplusJdbcDriver = new com.timeplus.proton.jdbc.ProtonDriver();
    private final Driver nativeDriver = new TimeplusDriver();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*")
                .warmupIterations(0)
                .measurementIterations(1)
                .forks(2)
                .build();

        new Runner(opt).run();
    }

    protected void withConnection(WithConnection withConnection, ConnectionType connectionType) throws Exception {

        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            DriverManager.deregisterDriver(drivers.nextElement());
        }

        int port;
        switch (connectionType) {
            case NATIVE:
                Class.forName("com.github.timeplus.jdbc.TimeplusDriver");
                DriverManager.registerDriver(nativeDriver);
                port = container.getMappedPort(TIMEPLUS_NATIVE_PORT);
                break;

            case JDBC:
                Class.forName("com.timeplus.proton.jdbc.ProtonDriver");
                DriverManager.registerDriver(timeplusJdbcDriver);
                port = container.getMappedPort(TIMEPLUS_HTTP_PORT);
                break;

            default:
                throw new RuntimeException("Never happen");
        }
        try (Connection connection = DriverManager.getConnection("jdbc:timeplus://" + container.getHost() + ":" + port)) {
            withConnection.apply(connection);
        }
    }

    protected void withStatement(Connection connection, AbstractITest.WithStatement withStatement) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            withStatement.apply(stmt);
        }
    }

    protected void withPreparedStatement(Connection connection,
                                         String sql,
                                         AbstractITest.WithPreparedStatement withPreparedStatement) throws Exception {
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            withPreparedStatement.apply(pstmt);
        }
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

    public enum ConnectionType {
        NATIVE, JDBC
    }
}
