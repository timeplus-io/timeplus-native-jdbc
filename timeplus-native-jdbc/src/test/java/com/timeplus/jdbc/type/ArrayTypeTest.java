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
import com.timeplus.misc.BytesHelper;
import com.timeplus.misc.DateTimeUtil;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
 
 public class ArrayTypeTest extends AbstractITest implements BytesHelper {
 
    @Test
    public void testArrayTypeInt() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS array_test");
            statement.execute("CREATE STREAM IF NOT EXISTS array_test (value array(int), nullable_value array(nullable(uint32))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO array_test (value, nullable_value) values(?,?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setArray(1, statement.getConnection().createArrayOf("int", new Integer[]{1, 2, 3}));
                    pstmt.setArray(2, statement.getConnection().createArrayOf("nullable(uint32)", new Long[]{1l, 2l, null}));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM array_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object[] objArray = (Object[]) rs.getArray(1).getArray();
                Integer[] value = Arrays.stream(objArray).map(Integer.class::cast).toArray(Integer[]::new);
                assertTrue(Arrays.equals(value, new Integer[]{1, 2, 3}));
                Object[] objArray2 = (Object[]) rs.getArray(2).getArray();
                Long[] value2 = Arrays.stream(objArray2).map(Long.class::cast).toArray(Long[]::new);
                assertTrue(Arrays.equals(value2, new Long[]{1l, 2l, null}));
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS array_test");
        });
    }

    @Test
    public void testArrayTypeString() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS array_test");
            statement.execute("CREATE STREAM IF NOT EXISTS array_test (value array(string), "
                                +" nullable_value array(nullable(string)), " 
                                +" valuefs array(fixed_string(5)), "
                                +" nullable_valuefs array(nullable(fixed_string(5)))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO array_test (value, nullable_value, valuefs, nullable_valuefs) values(?,?,?,?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setArray(1, statement.getConnection().createArrayOf("string", new String[]{"a", "b", "c"}));
                    pstmt.setArray(2, statement.getConnection().createArrayOf("nullable(string)", new String[]{"a", "b", null}));
                    pstmt.setArray(3, statement.getConnection().createArrayOf("fixed_string(5)", new String[]{"abcde", "$%^&$", "abcde"}));
                    pstmt.setArray(4, statement.getConnection().createArrayOf("nullable(fixed_string(5))", new String[]{"abcde", "$%^&$", null}));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM array_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object[] objArray = (Object[]) rs.getArray(1).getArray();
                String[] value = Arrays.stream(objArray).map(String.class::cast).toArray(String[]::new);
                assertTrue(Arrays.equals(value, new String[]{"a", "b", "c"}));
                Object[] objArray2 = (Object[]) rs.getArray(2).getArray();
                String[] value2 = Arrays.stream(objArray2).map(String.class::cast).toArray(String[]::new);
                assertTrue(Arrays.equals(value2, new String[]{"a", "b", null}));
                Object[] objArray3 = (Object[]) rs.getArray(3).getArray();
                String[] value3 = Arrays.stream(objArray3).map(String.class::cast).toArray(String[]::new);
                assertTrue(Arrays.equals(value3, new String[]{"abcde", "$%^&$", "abcde"}));
                Object[] objArray4 = (Object[]) rs.getArray(4).getArray();
                String[] value4 = Arrays.stream(objArray4).map(String.class::cast).toArray(String[]::new);
                assertTrue(Arrays.equals(value4, new String[]{"abcde", "$%^&$", null}));
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS array_test");
        });
    }

    @Test
    public void testArrayTypeDate() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS array_test");
            statement.execute("CREATE STREAM IF NOT EXISTS array_test (value array(date), "
                                +"value_date32 array(date32), "
                                +"value_datetime array(datetime), "
                                +"value_datetime64 array(datetime64(3)), "
                                +"nullable_value array(nullable(date)), "
                                +"nullable_value_date32 array(nullable(date32)), "
                                +"nullable_value_datetime array(nullable(datetime)), "
                                +"nullable_value_datetime64 array(nullable(datetime64(3)))) Engine=Memory()");

            Integer rowCnt = 300;
            Date temp1 = java.sql.Date.valueOf("2021-01-01");
            Date temp2 = java.sql.Date.valueOf("2021-01-02");
            Timestamp temp3 = java.sql.Timestamp.valueOf("2021-01-01 00:00:00");
            Timestamp temp4 = java.sql.Timestamp.valueOf("2021-01-02 00:00:00");

            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO array_test (value, value_date32, value_datetime, value_datetime64, nullable_value, nullable_value_date32, nullable_value_datetime, nullable_value_datetime64) values(?,?,?,?,?,?,?,?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setArray(1, statement.getConnection().createArrayOf("date", new Date[]{temp1, temp1, temp1}));
                    pstmt.setArray(2, statement.getConnection().createArrayOf("date32", new Date[]{temp2, temp2, temp2}));
                    pstmt.setArray(3, statement.getConnection().createArrayOf("datetime", new Timestamp[]{temp3, temp3, temp3}));
                    pstmt.setArray(4, statement.getConnection().createArrayOf("datetime64", new Timestamp[]{temp4, temp4, temp4}));
                    pstmt.setArray(5, statement.getConnection().createArrayOf("nullable(date)", new Date[]{temp1, temp1, null}));
                    pstmt.setArray(6, statement.getConnection().createArrayOf("nullable(date32)", new Date[]{temp2, temp2, null}));
                    pstmt.setArray(7, statement.getConnection().createArrayOf("nullable(datetime)", new Timestamp[]{temp3, temp3, null}));
                    pstmt.setArray(8, statement.getConnection().createArrayOf("nullable(datetime64)", new Timestamp[]{temp4, temp4, null}));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM array_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object[] objArray1 = (Object[]) rs.getArray(1).getArray();
                LocalDate[] value1 = Arrays.stream(objArray1).map(LocalDate.class::cast).toArray(LocalDate[]::new);
                Object[] objArray2 = (Object[]) rs.getArray(2).getArray();
                LocalDate[] value2 = Arrays.stream(objArray2).map(LocalDate.class::cast).toArray(LocalDate[]::new);             
                Object[] objArray3 = (Object[]) rs.getArray(3).getArray();
                Object[] value3 = Arrays.stream(objArray3).map(Object.class::cast).toArray(Object[]::new);
                Object[] objArray4 = (Object[]) rs.getArray(4).getArray();
                Object[] value4 = Arrays.stream(objArray4).map(Object.class::cast).toArray(Object[]::new);
                Object[] objArray5 = (Object[]) rs.getArray(5).getArray();
                LocalDate[] value5 = Arrays.stream(objArray5).map(LocalDate.class::cast).toArray(LocalDate[]::new);
                Object[] objArray6 = (Object[]) rs.getArray(6).getArray();
                LocalDate[] value6 = Arrays.stream(objArray6).map(LocalDate.class::cast).toArray(LocalDate[]::new);             
                Object[] objArray7 = (Object[]) rs.getArray(7).getArray();
                Object[] value7 = Arrays.stream(objArray7).map(Object.class::cast).toArray(Object[]::new);
                Object[] objArray8 = (Object[]) rs.getArray(8).getArray();
                Object[] value8 = Arrays.stream(objArray8).map(Object.class::cast).toArray(Object[]::new);

                assertEquals(Date.valueOf(value1[0]), temp1);
                assertEquals(Date.valueOf(value2[0]), temp2);
                assertEquals(DateTimeUtil.toTimestamp((ZonedDateTime) value3[0], null), temp3);
                assertEquals(DateTimeUtil.toTimestamp((ZonedDateTime) value4[0], null), temp4);
                assertEquals(Date.valueOf(value5[0]), temp1);
                assertEquals(Date.valueOf(value6[0]), temp2);
                assertEquals(DateTimeUtil.toTimestamp((ZonedDateTime) value7[0], null), temp3);
                assertEquals(DateTimeUtil.toTimestamp((ZonedDateTime) value8[0], null), temp4);
                assertNull(value5[2]);
                assertNull(value6[2]);
                assertNull(value7[2]);
                assertNull(value8[2]);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS array_test");
        });
    }

    @Test
    public void testArrayTypeDecimal() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS array_test");
            statement.execute("CREATE STREAM IF NOT EXISTS array_test (value array(decimal(5,3)), "
                                +"nullable_value array(nullable(decimal(76, 26)))) Engine=Memory()");
            BigDecimal[] valueArray = new BigDecimal[]{
                BigDecimal.valueOf(412341.21D).setScale(3, RoundingMode.HALF_UP),
                BigDecimal.valueOf(512341.25D).setScale(3, RoundingMode.HALF_UP)
            };
            BigDecimal[] valueArray_null = new BigDecimal[]{
                BigDecimal.valueOf(412341.21D).setScale(26, RoundingMode.HALF_UP),
                BigDecimal.valueOf(512341.25D).setScale(26, RoundingMode.HALF_UP),
                null
            };

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO array_test (value, nullable_value) values(?,?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setArray(1, statement.getConnection().createArrayOf("decimal(5,3)", valueArray));
                    pstmt.setArray(2, statement.getConnection().createArrayOf("nullable(decimal(76, 26))", valueArray_null));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM array_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object[] objArray = (Object[]) rs.getArray(1).getArray();
                BigDecimal[] value = Arrays.stream(objArray).map(BigDecimal.class::cast).toArray(BigDecimal[]::new);
                assertTrue(Arrays.equals(value, valueArray));
                Object[] objArray2 = (Object[]) rs.getArray(2).getArray();
                BigDecimal[] value2 = Arrays.stream(objArray2).map(BigDecimal.class::cast).toArray(BigDecimal[]::new);
                assertTrue(Arrays.equals(value2, valueArray_null));
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS array_test");
        });
    }

    @Test
    public void testArrayTypeEnum8() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS array_test");
            statement.execute("CREATE STREAM IF NOT EXISTS array_test (value array(enum8('test', 'test2')), "
                                +" nullable_value array(nullable(enum8('test', 'test2')))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO array_test (value, nullable_value) values(?,?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setArray(1, statement.getConnection().createArrayOf("string", new String[]{"test", "test", "test2"}));
                    pstmt.setArray(2, statement.getConnection().createArrayOf("nullable(string)", new String[]{"test", "test", null}));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM array_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object[] objArray = (Object[]) rs.getArray(1).getArray();
                String[] value = Arrays.stream(objArray).map(String.class::cast).toArray(String[]::new);
                assertTrue(Arrays.equals(value, new String[]{"test", "test", "test2"}));
                Object[] objArray2 = (Object[]) rs.getArray(2).getArray();
                String[] value2 = Arrays.stream(objArray2).map(String.class::cast).toArray(String[]::new);
                assertTrue(Arrays.equals(value2, new String[]{"test", "test", null}));
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS array_test");
        });
    }

    @Test
    public void testArrayTypeFloat() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS array_test");
            statement.execute("CREATE STREAM IF NOT EXISTS array_test (value array(float32), nullable_value array(nullable(float32))) Engine=Memory()");

            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO array_test (value, nullable_value) values(?,?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setArray(1, statement.getConnection().createArrayOf("float32", new Float[]{1.1f, 100.1f, 10000.1f}));
                    pstmt.setArray(2, statement.getConnection().createArrayOf("nullable(float32)", new Float[]{1.1f, 100.1f, null}));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM array_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object[] objArray = (Object[]) rs.getArray(1).getArray();
                Float[] value = Arrays.stream(objArray).map(Float.class::cast).toArray(Float[]::new);
                assertTrue(Arrays.equals(value, new Float[]{1.1f, 100.1f, 10000.1f}));
                Object[] objArray2 = (Object[]) rs.getArray(2).getArray();
                Float[] value2 = Arrays.stream(objArray2).map(Float.class::cast).toArray(Float[]::new);
                assertTrue(Arrays.equals(value2, new Float[]{1.1f, 100.1f, null}));
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS array_test");
        });
    }

    @Test
    public void testArrayTypeIP() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS array_test");
            statement.execute("CREATE STREAM IF NOT EXISTS array_test (value array(ipv4), "
                                +"nullable_value array(nullable(ipv4)), "
                                +"value6 array(ipv6), "
                                +"nullable_value6 array(nullable(ipv6))) Engine=Memory()");
            
            Long testIPv4Value1 = ipToLong("192.168.1.1");
            Long testIPv4Value2 = ipToLong("127.0.0.1");
            BigInteger testIPv6Value1 = new BigInteger("20010db885a3000000008a2e03707334", 16);
            BigInteger testIPv6Value2 = new BigInteger("1", 16);
            Integer rowCnt = 300;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO array_test (value, nullable_value, value6, nullable_value6) values(?,?,?,?);")) {
                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setArray(1, statement.getConnection().createArrayOf("int64", new Long[]{testIPv4Value1, testIPv4Value2, testIPv4Value2}));
                    pstmt.setArray(2, statement.getConnection().createArrayOf("nullable(int64)", new Long[]{testIPv4Value1, testIPv4Value2, null}));
                    pstmt.setArray(3, statement.getConnection().createArrayOf("int256", new BigInteger[]{testIPv6Value1, testIPv6Value2, testIPv6Value2}));
                    pstmt.setArray(4, statement.getConnection().createArrayOf("nullable(int256)", new BigInteger[]{testIPv6Value1, testIPv6Value2, null}));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM array_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object[] objArray = (Object[]) rs.getArray(1).getArray();
                Long[] value = Arrays.stream(objArray).map(Long.class::cast).toArray(Long[]::new);
                assertTrue(Arrays.equals(value, new Long[]{testIPv4Value1, testIPv4Value2, testIPv4Value2}));
                Object[] objArray2 = (Object[]) rs.getArray(2).getArray();
                Long[] value2 = Arrays.stream(objArray2).map(Long.class::cast).toArray(Long[]::new);
                assertTrue(Arrays.equals(value2, new Long[]{testIPv4Value1, testIPv4Value2, null}));
                Object[] objArray3 = (Object[]) rs.getArray(3).getArray();
                BigInteger[] value3 = Arrays.stream(objArray3).map(BigInteger.class::cast).toArray(BigInteger[]::new);
                assertTrue(Arrays.equals(value3, new BigInteger[]{testIPv6Value1, testIPv6Value2, testIPv6Value2}));
                Object[] objArray4 = (Object[]) rs.getArray(4).getArray();
                BigInteger[] value4 = Arrays.stream(objArray4).map(BigInteger.class::cast).toArray(BigInteger[]::new);
                assertTrue(Arrays.equals(value4, new BigInteger[]{testIPv6Value1, testIPv6Value2, null}));
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS array_test");
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
