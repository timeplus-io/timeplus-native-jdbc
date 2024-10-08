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

package com.timeplus.jdbc.statement;

import static com.timeplus.misc.ExceptionUtil.unchecked;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.timeplus.client.NativeContext;
import com.timeplus.data.Block;
import com.timeplus.data.IColumn;
import com.timeplus.data.IDataType;
import com.timeplus.data.type.DataTypeBool;
import com.timeplus.data.type.DataTypeDate;
import com.timeplus.data.type.DataTypeDate32;
import com.timeplus.data.type.DataTypeFloat32;
import com.timeplus.data.type.DataTypeFloat64;
import com.timeplus.data.type.DataTypeIPv4;
import com.timeplus.data.type.DataTypeIPv6;
import com.timeplus.data.type.DataTypeInt16;
import com.timeplus.data.type.DataTypeInt256;
import com.timeplus.data.type.DataTypeInt32;
import com.timeplus.data.type.DataTypeInt64;
import com.timeplus.data.type.DataTypeInt128;
import com.timeplus.data.type.DataTypeInt8;
import com.timeplus.data.type.DataTypeUInt128;
import com.timeplus.data.type.DataTypeUInt16;
import com.timeplus.data.type.DataTypeUInt256;
import com.timeplus.data.type.DataTypeUInt32;
import com.timeplus.data.type.DataTypeUInt64;
import com.timeplus.data.type.DataTypeUInt8;
import com.timeplus.data.type.DataTypeUUID;
import com.timeplus.data.type.complex.DataTypeArray;
import com.timeplus.data.type.complex.DataTypeDateTime;
import com.timeplus.data.type.complex.DataTypeDateTime64;
import com.timeplus.data.type.complex.DataTypeDecimal;
import com.timeplus.data.type.complex.DataTypeEnum16;
import com.timeplus.data.type.complex.DataTypeEnum8;
import com.timeplus.data.type.complex.DataTypeFixedString;
import com.timeplus.data.type.complex.DataTypeMap;
import com.timeplus.data.type.complex.DataTypeNothing;
import com.timeplus.data.type.complex.DataTypeNullable;
import com.timeplus.data.type.complex.DataTypeString;
import com.timeplus.data.type.complex.DataTypeTuple;
import com.timeplus.data.type.complex.DataTypeLowCardinality;
import com.timeplus.exception.TimeplusSQLException;
import com.timeplus.jdbc.TimeplusArray;
import com.timeplus.jdbc.TimeplusConnection;
import com.timeplus.jdbc.TimeplusStruct;
import com.timeplus.log.Logger;
import com.timeplus.log.LoggerFactory;
import com.timeplus.misc.BytesCharSeq;
import com.timeplus.misc.DateTimeUtil;
import com.timeplus.misc.ExceptionUtil;
import com.timeplus.misc.Validate;
import com.timeplus.stream.ValuesWithParametersNativeInputFormat;

public class TimeplusPreparedInsertStatement extends AbstractPreparedStatement {

    private static final Logger LOG = LoggerFactory.getLogger(TimeplusPreparedInsertStatement.class);

    private static int computeQuestionMarkSize(String query, int start) throws SQLException {
        int param = 0;
        boolean inQuotes = false, inBackQuotes = false;
        for (int i = 0; i < query.length(); i++) {
            char ch = query.charAt(i);
            if (ch == '`') {
                inBackQuotes = !inBackQuotes;
            } else if (ch == '\'') {
                inQuotes = !inQuotes;
            } else if (!inBackQuotes && !inQuotes) {
                if (ch == '?') {
                    Validate.isTrue(i > start, "");
                    param++;
                }
            }
        }
        return param;
    }

    private final int posOfData;
    private final String fullQuery;
    private final String insertQuery;
    private boolean blockInit;

    public TimeplusPreparedInsertStatement(int posOfData,
                                           String fullQuery,
                                           TimeplusConnection conn,
                                           NativeContext nativeContext) throws SQLException {
        super(conn, nativeContext, null);
        this.blockInit = false;
        this.posOfData = posOfData;
        this.fullQuery = fullQuery;
        this.insertQuery = fullQuery.substring(0, posOfData);

        initBlockIfPossible();
    }

    // paramPosition start with 1
    @Override
    public void setObject(int paramPosition, Object x) throws SQLException {
        initBlockIfPossible();
        int columnIdx = block.paramIdx2ColumnIdx(paramPosition - 1);
        IColumn column = block.getColumn(columnIdx);
        block.setObject(columnIdx, convertToNativeDataType(column.type(), x));
    }

    @Override
    public boolean execute() throws SQLException {
        return executeQuery() != null;
    }

    @Override
    public int executeUpdate() throws SQLException {
        addParameters();
        int result = connection.sendInsertRequest(block);
        this.blockInit = false;
        this.block.cleanup();
        return result;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        executeUpdate();
        return null;
    }

    @Override
    public void addBatch() throws SQLException {
        addParameters();
    }

    @Override
    public void clearBatch() throws SQLException {
    }

    @Override
    public int[] executeBatch() throws SQLException {
        int rows = connection.sendInsertRequest(block);
        int[] result = new int[rows];
        Arrays.fill(result, 1);
        clearBatch();
        this.blockInit = false;
        this.block.cleanup();
        return result;
    }

    @Override
    public void close() throws SQLException {
        if (blockInit) {
            // Empty insert when close.
            this.connection.sendInsertRequest(new Block());
            this.blockInit = false;
        }
        // clean up block on close
        this.block.cleanup();
        super.close();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(": ");
        try {
            sb.append(insertQuery).append(" (");
            for (int i = 0; i < block.columnCnt(); i++) {
                Object obj = block.getObject(i);
                if (obj == null) {
                    sb.append("?");
                } else if (obj instanceof Number) {
                    sb.append(obj);
                } else {
                    sb.append("'").append(obj).append("'");
                }
                if (i < block.columnCnt() - 1) {
                    sb.append(",");
                }
            }
            sb.append(")");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    private void initBlockIfPossible() throws SQLException {
        if (this.blockInit) {
            return;
        }
        ExceptionUtil.rethrowSQLException(() -> {
            this.block = connection.getSampleBlock(insertQuery);
            this.block.initWriteBuffer();
            this.blockInit = true;
            new ValuesWithParametersNativeInputFormat(posOfData, fullQuery).fill(block);
        });
    }

    private void addParameters() throws SQLException {
        block.appendRow();
    }

    // TODO we actually need a type cast system rather than put all type cast stuffs here
    private Object convertToNativeDataType(IDataType<?, ?> type, Object obj) throws TimeplusSQLException {
        if (obj == null) {
            if (type.nullable() || type instanceof DataTypeNothing)
                return null;
            throw new TimeplusSQLException(-1, "type[" + type.name() + "] doesn't support null value");
        }
        // put the most common cast at first to avoid `instanceof` test overhead
        if (type instanceof DataTypeString || type instanceof DataTypeFixedString) {
            if (obj instanceof CharSequence)
                return obj;
            if (obj instanceof byte[])
                return new BytesCharSeq((byte[]) obj);
            String objStr = obj.toString();
            LOG.debug("set value[{}]: {} on String Column", obj.getClass(), obj);
            return objStr;
        }
        if (type instanceof DataTypeDate) {
            if (obj instanceof java.util.Date)
                return ((Date) obj).toLocalDate();
            if (obj instanceof LocalDate)
                return obj;
        }

        if (type instanceof DataTypeDate32) {
            if (obj instanceof java.util.Date)
                return ((Date) obj).toLocalDate();
            if (obj instanceof LocalDate)
                return obj;
        }
        // TODO support
        //   1. other Java8 time, i.e. OffsetDateTime, Instant
        //   2. unix timestamp, but in second or millisecond?
        if (type instanceof DataTypeDateTime || type instanceof DataTypeDateTime64) {
            if (obj instanceof Timestamp)
                return DateTimeUtil.toZonedDateTime((Timestamp) obj, tz);
            if (obj instanceof LocalDateTime)
                return ((LocalDateTime) obj).atZone(tz);
            if (obj instanceof ZonedDateTime)
                return obj;
        }
        if (type instanceof DataTypeInt8) {
            if (obj instanceof Number)
                return ((Number) obj).byteValue();
        }
        if (type instanceof DataTypeBool) {
            if (obj instanceof Boolean)
                return ((Boolean) obj).booleanValue();
        }
        if (type instanceof DataTypeUInt8 || type instanceof DataTypeInt16) {
            if (obj instanceof Number)
                return ((Number) obj).shortValue();
        }
        if (type instanceof DataTypeUInt16 || type instanceof DataTypeInt32) {
            if (obj instanceof Number)
                return ((Number) obj).intValue();
        }
        if (type instanceof DataTypeUInt32 || type instanceof DataTypeInt64) {
            if (obj instanceof Number)
                return ((Number) obj).longValue();
        }
        if (type instanceof DataTypeUInt64 || type instanceof DataTypeInt128 || type instanceof DataTypeInt256 || type instanceof DataTypeUInt128 || type instanceof DataTypeUInt256 || type instanceof DataTypeIPv6) {
            if (obj instanceof BigInteger)
                return obj;
            if (obj instanceof BigDecimal)
                return ((BigDecimal) obj).toBigInteger();
            if (obj instanceof Number)
                return BigInteger.valueOf(((Number) obj).longValue());
        }
        if (type instanceof DataTypeFloat32) {
            if (obj instanceof Number)
                return ((Number) obj).floatValue();
        }
        if (type instanceof DataTypeFloat64) {
            if (obj instanceof Number)
                return ((Number) obj).doubleValue();
        }
        if (type instanceof DataTypeDecimal) {
            if (obj instanceof BigDecimal)
                return obj;
            if (obj instanceof BigInteger)
                return new BigDecimal((BigInteger) obj);
            if (obj instanceof Number)
                return ((Number) obj).doubleValue();
        }
        if (type instanceof DataTypeUUID) {
            if (obj instanceof UUID)
                return obj;
            if (obj instanceof String) {
                return UUID.fromString((String) obj);
            }
        }
        if (type instanceof DataTypeEnum8 || type instanceof DataTypeEnum16) {
            if (obj instanceof String)
                return obj;
        }
        if (type instanceof DataTypeIPv4) {
            if (obj instanceof Long)
                return obj;
            if (obj instanceof Number)
                return ((Number) obj).longValue();
        }
        if (type instanceof DataTypeBool) {
            if (obj instanceof Byte)
                return obj;
            if (obj instanceof Boolean)
                return (byte) obj;
        }
        if (type instanceof DataTypeNothing) {
            return null;
        }
        if (type instanceof DataTypeNullable) {
            // handled null at first, so obj also not null here
            return convertToNativeDataType(((DataTypeNullable) type).getNestedDataType(), obj);
        }
        if (type instanceof DataTypeLowCardinality) {
            return convertToNativeDataType(((DataTypeLowCardinality) type).getNestedTypes(), obj);
        }
        if (type instanceof DataTypeArray) {
            if (!(obj instanceof TimeplusArray)) {
                throw new TimeplusSQLException(-1, "require TimeplusArray for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((TimeplusArray) obj).mapElements(unchecked(this::convertToNativeDataType));
        }
        if (type instanceof DataTypeTuple) {
            if (!(obj instanceof TimeplusStruct)) {
                throw new TimeplusSQLException(-1, "require TimeplusStruct for column: " + type.name() + ", but found " + obj.getClass());
            }
            return ((TimeplusStruct) obj).mapAttributes(((DataTypeTuple) type).getNestedTypes(), unchecked(this::convertToNativeDataType));
        }
        if (type instanceof DataTypeMap) {
            if (obj instanceof Map) {
                // return obj;
                Map<Object, Object> result = new HashMap<Object, Object>();
                IDataType<?, ?>[] nestedTypes = ((DataTypeMap) type).getNestedTypes();
                Map<?, ?> dataMap = (Map<?, ?>) obj;
                for (Entry<?, ?> entry : dataMap.entrySet()) {
                    Object key = convertToNativeDataType(nestedTypes[0], entry.getKey());
                    Object value = convertToNativeDataType(nestedTypes[1], entry.getValue());
                    result.put(key, value);
                }
                return result;
            } else {
                throw new TimeplusSQLException(-1, "require Map for column: " + type.name() + ", but found " + obj.getClass());
            }
        }
        
        LOG.debug("unhandled type: {}[{}]", type.name(), obj.getClass());
        return obj;
    }
}
