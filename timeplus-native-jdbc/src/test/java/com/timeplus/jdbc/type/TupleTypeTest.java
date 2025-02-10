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

package com.timeplus.jdbc.type;

import com.timeplus.jdbc.AbstractITest;
import com.timeplus.jdbc.TimeplusStruct;
import com.timeplus.misc.BytesHelper;
import com.timeplus.misc.DateTimeUtil;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TupleTypeTest extends AbstractITest implements BytesHelper {

    // test for tuple(int, string)
    @Test
    public void testTupleType_AllInOne() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS tuple_test");
            statement.execute("CREATE STREAM IF NOT EXISTS tuple_test (value tuple(int8, int16, int32, int64, "
                                +"int128, int256, uint8, uint16, uint32, uint64, uint128, uint256, string, fixed_string(5), "
                                +"date, datetime, decimal(5, 3), decimal(15, 5), decimal(38, 16), decimal(76, 26), "
                                +"enum8('test','test2'), enum16('test','test2'), float32, float64, ipv4, ipv6, bool, uuid)) Engine=Memory()");

            Integer rowCnt = 300;
            Long testIPv4Value1 = ipToLong("192.168.1.1");
            BigInteger testIPv6Value1 = new BigInteger("20010db885a3000000008a2e03707334", 16);
            java.util.UUID uuid = java.util.UUID.randomUUID();
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO tuple_test (value) values(?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    Object[] tupleValue = new Object[]{(byte) 1, (short) 1, 1, 1l, testIPv6Value1, testIPv6Value1, (short) 1, 1, 1l, new BigInteger("1"), 
                                                        testIPv6Value1, testIPv6Value1, "this_is_key", "a_key", java.sql.Date.valueOf("1970-01-01"), 
                                                        java.sql.Timestamp.valueOf("2021-01-01 00:00:00"), 
                                                        BigDecimal.valueOf(412341.21D).setScale(3, RoundingMode.HALF_UP), 
                                                        BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP), 
                                                        BigDecimal.valueOf(412341.21D).setScale(16, RoundingMode.HALF_UP), 
                                                        BigDecimal.valueOf(412341.21D).setScale(26, RoundingMode.HALF_UP), 
                                                        "test", "test", 1.1f, 100.1, testIPv4Value1, testIPv6Value1, (byte) 1, uuid};
                    TimeplusStruct tuple = new TimeplusStruct("tuple", tupleValue);
                    // convert to TimeplusStruct

                    pstmt.setObject(1, tuple);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM tuple_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object obj1 = rs.getObject(1);
                TimeplusStruct tuple = (TimeplusStruct) obj1;
                Object[] tupleValue = (Object[]) tuple.getAttributes();
                assertEquals(tupleValue[0], (byte) 1);
                assertEquals(tupleValue[1], (short) 1);
                assertEquals(tupleValue[2], 1);
                assertEquals(tupleValue[3], 1l);
                assertEquals(tupleValue[4], testIPv6Value1);
                assertEquals(tupleValue[5], testIPv6Value1);
                assertEquals(tupleValue[6], (short) 1);
                assertEquals(tupleValue[7], 1);
                assertEquals(tupleValue[8], 1l);
                assertEquals(tupleValue[9], new BigInteger("1"));
                assertEquals(tupleValue[10], testIPv6Value1);
                assertEquals(tupleValue[11], testIPv6Value1);
                assertEquals(tupleValue[12], "this_is_key");
                assertEquals(tupleValue[13], "a_key");
                assertEquals(tupleValue[14].toString(), "1970-01-01");
                assertEquals(DateTimeUtil.toTimestamp((ZonedDateTime) tupleValue[15], null), java.sql.Timestamp.valueOf("2021-01-01 00:00:00"));
                assertEquals(tupleValue[16], BigDecimal.valueOf(412341.21D).setScale(3, RoundingMode.HALF_UP));
                assertEquals(tupleValue[17], BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP));
                assertEquals(tupleValue[18], BigDecimal.valueOf(412341.21D).setScale(16, RoundingMode.HALF_UP));
                assertEquals(tupleValue[19], BigDecimal.valueOf(412341.21D).setScale(26, RoundingMode.HALF_UP));
                assertEquals(tupleValue[20], "test");
                assertEquals(tupleValue[21], "test");
                assertEquals(tupleValue[22], 1.1f);
                assertEquals(tupleValue[23], 100.1);
                assertEquals(tupleValue[24], testIPv4Value1);
                assertEquals(tupleValue[25], testIPv6Value1);
                assertEquals(tupleValue[26], (byte) 1);
                assertEquals(tupleValue[27], uuid);
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS tuple_test");
        });
    }

    @Test
    public void testTupleType_Nullable() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS tuple_test");
            statement.execute("CREATE STREAM IF NOT EXISTS tuple_test (value tuple(nullable(int), "
                                +"nullable(string), nullable(fixed_string(5)), "
                                +"nullable(date), nullable(datetime), "
                                +"nullable(decimal(15, 5)), nullable(enum8('test','test2')), "
                                +"nullable(float32), nullable(ipv4), nullable(ipv6), nullable(bool), nullable(uuid))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO tuple_test (value) values(?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    Object[] tupleValueNull = new Object[]{null, null, null, null, null, null, null, null, null, null, null, null};
                    TimeplusStruct tupleNull = new TimeplusStruct("tuple", tupleValueNull);
                    pstmt.setObject(1, tupleNull);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM tuple_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object obj1 = rs.getObject(1);
                TimeplusStruct tuple = (TimeplusStruct) obj1;
                Object[] tupleValue = (Object[]) tuple.getAttributes();
                for (int i = 0; i < 12; i++) {
                    assertEquals(tupleValue[i], null);
                }
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS tuple_test");
        });
    }

    @Test
    public void testTupleType_MultipleScenerio() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS tuple_test");
            statement.execute("CREATE STREAM IF NOT EXISTS tuple_test (value tuple(nullable(int), nullable(int), "
                                +"nullable(string), nullable(string), nullable(fixed_string(5)), nullable(fixed_string(5)), "
                                +"nullable(date), nullable(date), nullable(datetime), nullable(datetime), "
                                +"nullable(decimal(15, 5)), nullable(decimal(15, 5)), nullable(enum8('test','test2')), "
                                +"nullable(enum8('test','test2')), nullable(float32), nullable(float32), "
                                +"nullable(ipv4), nullable(ipv4), nullable(ipv6), nullable(ipv6), nullable(bool), nullable(bool), nullable(uuid), nullable(uuid))) Engine=Memory()");

            Integer rowCnt = 300;
            Long testIPv4Value1 = ipToLong("192.168.1.1");
            BigInteger testIPv6Value1 = new BigInteger("20010db885a3000000008a2e03707334", 16);
            java.util.UUID uuid = java.util.UUID.randomUUID();
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO tuple_test (value) values(?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    Object[] tupleValueNull = new Object[]{null, 1, null, "this_is_key", null, "a_key", null, 
                                                            java.sql.Date.valueOf("1970-01-01"), null, 
                                                            java.sql.Timestamp.valueOf("2021-01-01 00:00:00"), null, 
                                                            BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP), null, 
                                                            "test", null, 1.1f, null, testIPv4Value1, null, testIPv6Value1, null, (byte) 1, null, uuid};
                    TimeplusStruct tupleNull = new TimeplusStruct("tuple", tupleValueNull);
                    pstmt.setObject(1, tupleNull);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM tuple_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object obj1 = rs.getObject(1);
                TimeplusStruct tuple = (TimeplusStruct) obj1;
                Object[] tupleValue = (Object[]) tuple.getAttributes();
                for (int i = 0; i < 24; i++) {
                    if (i % 2 == 0) {
                        assertEquals(tupleValue[i], null);
                    }
                }
                assertEquals(tupleValue[1], 1);
                assertEquals(tupleValue[3], "this_is_key");
                assertEquals(tupleValue[5], "a_key");
                assertEquals(tupleValue[7].toString(), "1970-01-01");
                assertEquals(DateTimeUtil.toTimestamp((ZonedDateTime) tupleValue[9], null), java.sql.Timestamp.valueOf("2021-01-01 00:00:00"));
                assertEquals(tupleValue[11], BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP));
                assertEquals(tupleValue[13], "test");
                assertEquals(tupleValue[15], 1.1f);
                assertEquals(tupleValue[17], testIPv4Value1);
                assertEquals(tupleValue[19], testIPv6Value1);
                assertEquals(tupleValue[21], (byte) 1);
                assertEquals(tupleValue[23], uuid);
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS tuple_test");
        });
    }

    @Test
    public void testTupleWithName() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP DICTIONARY IF EXISTS tuple_test");
            statement.execute("CREATE DICTIONARY tuple_test (LocationID uint16 DEFAULT 0,"
                                +"Borough string, Zone string, service_zone string) PRIMARY KEY LocationID\n"
                                +"SOURCE(HTTP(URL 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/taxi_zone_lookup.csv'"
                                +"FORMAT 'CSVWithNames')) \n LIFETIME(MIN 0 MAX 0) LAYOUT(HASHED_ARRAY());");

            ResultSet rs = statement.executeQuery("SELECT dict_get('tuple_test', ('Borough','Zone'), 132)");
            while (rs.next()) {
                ResultSetMetaData data = rs.getMetaData();
                String name = data.getColumnTypeName(1);
                assertEquals("tuple(Borough string,Zone string)", name);
            }
            statement.execute("DROP DICTIONARY IF EXISTS tuple_test");
        });
    }

    public long ipToLong(String ipAddress) {
        String[] ipAddressInArray = ipAddress.split("\\.");

        long result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {
            int power = 3 - i;
            int ip = Integer.parseInt(ipAddressInArray[i]);
            result += ip * Math.pow(256, power);
        }

        return result;
    }

}
