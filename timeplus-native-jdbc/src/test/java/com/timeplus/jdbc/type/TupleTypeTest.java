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
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TupleTypeTest extends AbstractITest implements BytesHelper {

    // test for tuple(int, string)
    @Test
    public void testTupleType_AllInOne() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS tuple_test");
            statement.execute("CREATE STREAM IF NOT EXISTS tuple_test (value tuple(int, string, fixed_string(5), "
                                +"date, datetime, decimal(15, 5), enum8('test','test2'), float32, ipv4, ipv6)) Engine=Memory()");

            Integer rowCnt = 300;
            Long testIPv4Value1 = ipToLong("192.168.1.1");
            BigInteger testIPv6Value1 = new BigInteger("20010db885a3000000008a2e03707334", 16);
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO tuple_test (value) values(?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    Object[] tupleValue = new Object[]{1, "this_is_key", "a_key", java.sql.Date.valueOf("1970-01-01"), java.sql.Timestamp.valueOf("2021-01-01 00:00:00"), BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP), "test", 1.1f, testIPv4Value1, testIPv6Value1};
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
                assertEquals(tupleValue[0], 1);
                assertEquals(tupleValue[1], "this_is_key");
                assertEquals(tupleValue[2], "a_key");
                assertEquals(tupleValue[3].toString(), "1970-01-01");
                assertEquals(DateTimeUtil.toTimestamp((ZonedDateTime) tupleValue[4], null), java.sql.Timestamp.valueOf("2021-01-01 00:00:00"));
                assertEquals(tupleValue[5], BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP));
                assertEquals(tupleValue[6], "test");
                assertEquals(tupleValue[7], 1.1f);
                assertEquals(tupleValue[8], testIPv4Value1);
                assertEquals(tupleValue[9], testIPv6Value1);
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
                                +"nullable(float32), nullable(ipv4), nullable(ipv6))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO tuple_test (value) values(?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    Object[] tupleValueNull = new Object[]{null, null, null, null, null, null, null, null, null, null};
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
                for (int i = 0; i < 10; i++) {
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
                                +"nullable(ipv4), nullable(ipv4), nullable(ipv6), nullable(ipv6))) Engine=Memory()");

            Integer rowCnt = 300;
            Long testIPv4Value1 = ipToLong("192.168.1.1");
            BigInteger testIPv6Value1 = new BigInteger("20010db885a3000000008a2e03707334", 16);
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO tuple_test (value) values(?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    Object[] tupleValueNull = new Object[]{null, 1, null, "this_is_key", null, "a_key", null, 
                                                            java.sql.Date.valueOf("1970-01-01"), null, 
                                                            java.sql.Timestamp.valueOf("2021-01-01 00:00:00"), null, 
                                                            BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP), null, 
                                                            "test", null, 1.1f, null, testIPv4Value1, null, testIPv6Value1};
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
                for (int i = 0; i < 20; i++) {
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
            }
            assertEquals(size, rowCnt);
            statement.execute("DROP STREAM IF EXISTS tuple_test");
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
