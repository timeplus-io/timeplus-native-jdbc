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

import com.timeplus.client.NativeClient;
import com.timeplus.client.NativeContext;
import com.timeplus.client.SessionState;
import com.timeplus.data.Block;
import com.timeplus.data.DataTypeFactory;
import com.timeplus.jdbc.statement.TimeplusPreparedInsertStatement;
import com.timeplus.jdbc.statement.TimeplusPreparedQueryStatement;
import com.timeplus.jdbc.statement.TimeplusStatement;
import com.timeplus.jdbc.wrapper.SQLConnection;
import com.timeplus.log.Logger;
import com.timeplus.log.LoggerFactory;
import com.timeplus.misc.Validate;
import com.timeplus.protocol.HelloResponse;
import com.timeplus.settings.TimeplusConfig;
import com.timeplus.settings.TimeplusDefines;
import com.timeplus.stream.QueryResult;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.sql.Array;
import java.sql.ClientInfoStatus;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.timeplus.jdbc.TimeplusJdbcUrlParser.PORT_DELIMITER;

public class TimeplusConnection implements SQLConnection {

    private static final Logger LOG = LoggerFactory.getLogger(TimeplusConnection.class);
    private static final Pattern VALUES_REGEX = Pattern.compile("[Vv][Aa][Ll][Uu][Ee][Ss]\\s*\\(");

    private final AtomicBoolean isClosed;
    private final AtomicReference<TimeplusConfig> cfg;
    // TODO move to NativeClient
    private final AtomicReference<SessionState> state = new AtomicReference<>(SessionState.IDLE);
    private volatile NativeContext nativeCtx;

    protected TimeplusConnection(TimeplusConfig cfg, NativeContext nativeCtx) {
        this.isClosed = new AtomicBoolean(false);
        this.cfg = new AtomicReference<>(cfg);
        this.nativeCtx = nativeCtx;
    }

    public TimeplusConfig cfg() {
        return cfg.get();
    }

    public NativeContext.ServerContext serverContext() {
        return nativeCtx.serverCtx();
    }

    public NativeContext.ClientContext clientContext() {
        return nativeCtx.clientCtx();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public void commit() throws SQLException {
    }

    @Override
    public void rollback() throws SQLException {
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.close();
    }

    @Override
    public void close() throws SQLException {
        if (!isClosed() && isClosed.compareAndSet(false, true)) {
            NativeClient nativeClient = nativeCtx.nativeClient();
            nativeClient.disconnect();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed.get();
    }

    @Override
    public Statement createStatement() throws SQLException {
        Validate.isTrue(!isClosed(), "Unable to create Statement, because the connection is closed.");
        return new TimeplusStatement(this, nativeCtx);
    }

    @Override
    public PreparedStatement prepareStatement(String query) throws SQLException {
        Validate.isTrue(!isClosed(), "Unable to create PreparedStatement, because the connection is closed.");
        Matcher matcher = VALUES_REGEX.matcher(query);
        return matcher.find() ? new TimeplusPreparedInsertStatement(matcher.end() - 1, query, this, nativeCtx) :
                new TimeplusPreparedQueryStatement(this, nativeCtx, query);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return this.prepareStatement(sql);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            cfg.set(TimeplusConfig.Builder.builder(cfg.get()).withProperties(properties).build());
        } catch (Exception ex) {
            Map<String, ClientInfoStatus> failed = new HashMap<>();
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                failed.put((String) entry.getKey(), ClientInfoStatus.REASON_UNKNOWN);
            }
            throw new SQLClientInfoException(failed, ex);
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        Properties properties = new Properties();
        properties.put(name, value);
        this.setClientInfo(properties);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        Validate.isTrue(!isClosed(), "Unable to create Array, because the connection is closed.");
        return new TimeplusArray(DataTypeFactory.get(typeName, nativeCtx.serverCtx()), elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        Validate.isTrue(!isClosed(), "Unable to create Struct, because the connection is closed.");
        return new TimeplusStruct(typeName, attributes);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return getNativeClient().ping(Duration.ofSeconds(timeout), nativeCtx.serverCtx());
    }

    // Timeplus support only `database`, we treat it as JDBC `schema`
    @Override
    public void setSchema(String schema) throws SQLException {
        this.cfg.set(this.cfg().withDatabase(schema));
    }

    @Override
    @Nullable
    public String getSchema() throws SQLException {
        return this.cfg().database();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        // do nothing
    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new TimeplusDatabaseMetadata(cfg().jdbcUrl(), this);
    }

    @Override
    public Logger logger() {
        return TimeplusConnection.LOG;
    }

    public boolean ping(Duration timeout) throws SQLException {
        return nativeCtx.nativeClient().ping(timeout, nativeCtx.serverCtx());
    }

    public Block getSampleBlock(final String insertQuery) throws SQLException {
        NativeClient nativeClient = getHealthyNativeClient();
        nativeClient.sendQuery(insertQuery, nativeCtx.clientCtx(), cfg.get().settings());
        Validate.isTrue(this.state.compareAndSet(SessionState.IDLE, SessionState.WAITING_INSERT),
                "Connection is currently waiting for an insert operation, check your previous InsertStatement.");
        return nativeClient.receiveSampleBlock(cfg.get().queryTimeout(), nativeCtx.serverCtx());
    }

    public QueryResult sendQueryRequest(final String query, TimeplusConfig cfg) throws SQLException {
        Validate.isTrue(this.state.get() == SessionState.IDLE,
                "Connection is currently waiting for an insert operation, check your previous InsertStatement.");
        NativeClient nativeClient = getHealthyNativeClient();
        nativeClient.sendQuery(query, nativeCtx.clientCtx(), cfg.settings());
        return nativeClient.receiveQuery(cfg.queryTimeout(), nativeCtx.serverCtx());
    }
    // when sendInsertRequest we must ensure the connection is healthy
    // the #getSampleBlock() must be called before this method

    public int sendInsertRequest(Block block) throws SQLException {
        Validate.isTrue(this.state.get() == SessionState.WAITING_INSERT, "Call getSampleBlock before insert.");
        try {
            NativeClient nativeClient = getNativeClient();
            nativeClient.sendData(block);
            nativeClient.sendData(new Block());
            nativeClient.receiveEndOfStream(cfg.get().queryTimeout(), nativeCtx.serverCtx());
        } finally {
            Validate.isTrue(this.state.compareAndSet(SessionState.WAITING_INSERT, SessionState.IDLE));
        }
        return block.rowCnt();
    }
    
    private synchronized NativeClient getHealthyNativeClient() throws SQLException {
        NativeContext oldCtx = nativeCtx;
        if (!oldCtx.nativeClient().ping(cfg.get().queryTimeout(), nativeCtx.serverCtx())) {
            LOG.warn("connection loss with state[{}], create new connection and reset state", state);
            nativeCtx = createNativeContext(cfg.get());
            state.set(SessionState.IDLE);
            oldCtx.nativeClient().silentDisconnect();
        }

        return nativeCtx.nativeClient();
    }

    private NativeClient getNativeClient() {
        return nativeCtx.nativeClient();
    }

    public static TimeplusConnection createTimeplusConnection(TimeplusConfig configure) throws SQLException {
        return new TimeplusConnection(configure, createNativeContext(configure));
    }

    private static NativeContext createNativeContext(TimeplusConfig configure) throws SQLException {
        if (configure.hosts().size() == 1) {
            NativeClient nativeClient = NativeClient.connect(configure);
            return new NativeContext(clientContext(nativeClient, configure), serverContext(nativeClient, configure), nativeClient);
        }

        return createFailoverNativeContext(configure);
    }

    private static NativeContext createFailoverNativeContext(TimeplusConfig configure) throws SQLException {
        NativeClient nativeClient = null;
        SQLException lastException = null;

        int tryIndex = 0;
        do {
            String hostAndPort = configure.hosts().get(tryIndex);
            String[] hostAndPortSplit = hostAndPort.split(PORT_DELIMITER, 2);
            String host = hostAndPortSplit[0];
            int port;

            if (hostAndPortSplit.length == 2) {
                port = Integer.parseInt(hostAndPortSplit[1]);
            } else {
                port = configure.port();
            }

            try {
                nativeClient = NativeClient.connect(host, port, configure);
            } catch (SQLException e) {
                lastException = e;
            }
            tryIndex++;
        } while (nativeClient == null && tryIndex < configure.hosts().size());

        if (nativeClient == null) {
            throw lastException;
        }

        return new NativeContext(clientContext(nativeClient, configure), serverContext(nativeClient, configure), nativeClient);
    }

    private static NativeContext.ClientContext clientContext(NativeClient nativeClient, TimeplusConfig configure) throws SQLException {
        Validate.isTrue(nativeClient.address() instanceof InetSocketAddress);
        InetSocketAddress address = (InetSocketAddress) nativeClient.address();
        String clientName = configure.clientName();
        String initialAddress = "[::ffff:127.0.0.1]:0";
        return new NativeContext.ClientContext(initialAddress, address.getHostName(), clientName);
    }

    private static NativeContext.ServerContext serverContext(NativeClient nativeClient, TimeplusConfig configure) throws SQLException {
        try {
            long revision = TimeplusDefines.CLIENT_REVISION;
            nativeClient.sendHello("client", revision, configure.database(), configure.user(), configure.password());

            HelloResponse response = nativeClient.receiveHello(configure.queryTimeout(), null);
            ZoneId timeZone = ZoneId.of(response.serverTimeZone());
            return new NativeContext.ServerContext(
                    response.majorVersion(), response.minorVersion(), response.reversion(),
                    configure, timeZone, response.serverDisplayName());
        } catch (SQLException rethrows) {
            nativeClient.silentDisconnect();
            throw rethrows;
        }
    }
}
