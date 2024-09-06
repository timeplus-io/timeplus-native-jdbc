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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapTypeTest extends AbstractITest implements BytesHelper {

    // test for map(string, int)
    /**
     * @throws Exception
     */
    @Test
    public void testMapType_AllValue() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS map_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS map_test (value map(string, int),"
                    +"values map(string, string), "
                    +"valuefs map(string, fixed_string(5)), "
                    +"value_date map(string, date), "
                    +"value_datetime map(string, datetime), "
                    +"value_decimal32 map(string, decimal(5,3)), "
                    +"value_decimal64 map(string, decimal(15, 5)), "
                    +"value_decimal128 map(string, decimal(38, 16)), "
                    +"value_decimal256 map(string, decimal(76, 26)), "
                    +"value_enum map(string, enum8('test','test2')), "
                    +"value_enum16 map(string, enum16('test','test2')), "
                    +"value_float32 map(string, float32), "
                    +"value_float64 map(string, float64), "
                    +"value_ipv4 map(string, ipv4), "
                    +"value_ipv6 map(string, ipv6), "
                    +"value_boolean map(string, boolean), "
                    +"value_uuid map(string, uuid)) Engine=Memory()");

            Integer rowCnt = 100;
            Long testIPv4Value1 = ipToLong("192.168.1.1");
            BigInteger testIPv6Value1 = new BigInteger("20010db885a3000000008a2e03707334", 16);
            java.util.UUID uuid = java.util.UUID.randomUUID();
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO map_test (value, values, valuefs, value_date, value_datetime, value_decimal32, value_decimal64, value_decimal128, "
                    +"value_decimal256, value_enum, value_enum16, value_float32, value_float64, value_ipv4, value_ipv6, value_boolean, value_uuid) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")) {
                
            Map<String, Integer> map = new HashMap<>();
            map.put("key", 1);
            Map<String, String> maps = new HashMap<>();
            maps.put("key", "this_is_key");
            Map<String, String> mapfs = new HashMap<>();
            mapfs.put("key", "a_key");
            Map<String, Date> map_date = new HashMap<>();
            map_date.put("key", Date.valueOf("1970-01-01"));
            Map<String, java.sql.Timestamp> map_datetime = new HashMap<>();
            map_datetime.put("key", java.sql.Timestamp.valueOf("2021-01-01 00:00:00"));
            Map<String, BigDecimal> map_decimal32 = new HashMap<>();
            map_decimal32.put("key", BigDecimal.valueOf(412341.21D).setScale(3, RoundingMode.HALF_UP));
            Map<String, BigDecimal> map_decimal64 = new HashMap<>();
            map_decimal64.put("key", BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP));
            Map<String, BigDecimal> map_decimal128 = new HashMap<>();
            map_decimal128.put("key", BigDecimal.valueOf(412341.21D).setScale(16, RoundingMode.HALF_UP));
            Map<String, BigDecimal> map_decimal256 = new HashMap<>();
            map_decimal256.put("key", BigDecimal.valueOf(412341.21D).setScale(26, RoundingMode.HALF_UP));
            Map<String, String> map_enum = new HashMap<>();
            map_enum.put("key", "test");
            Map<String, Float> map_float32 = new HashMap<>();
            map_float32.put("key", 1.1f);
            Map<String, Double> map_float64 = new HashMap<>();
            map_float64.put("key", 1.1);
            Map<String, Long> map_ipv4 = new HashMap<>();
            map_ipv4.put("key", testIPv4Value1);
            Map<String, BigInteger> map_ipv6 = new HashMap<>();
            map_ipv6.put("key", testIPv6Value1);
            Map<String, Byte> map_bool = new HashMap<>();
            map_bool.put("key", (byte) 1);
            Map<String, java.util.UUID> map_uuid = new HashMap<>();
            map_uuid.put("key", uuid);

                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setObject(1, map);
                    pstmt.setObject(2, maps);
                    pstmt.setObject(3, mapfs);
                    pstmt.setObject(4, map_date);
                    pstmt.setObject(5, map_datetime);
                    pstmt.setObject(6, map_decimal32);
                    pstmt.setObject(7, map_decimal64);
                    pstmt.setObject(8, map_decimal128);
                    pstmt.setObject(9, map_decimal256);
                    pstmt.setObject(10, map_enum);
                    pstmt.setObject(11, map_enum);
                    pstmt.setObject(12, map_float32);
                    pstmt.setObject(13, map_float64);
                    pstmt.setObject(14, map_ipv4);
                    pstmt.setObject(15, map_ipv6);
                    pstmt.setObject(16, map_bool);
                    pstmt.setObject(17, map_uuid);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM map_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                // check the value
                Map<String, Integer> value = (Map<String, Integer>) rs.getObject(1);
                assertEquals(value.get("key"), 1);
                Map<String, String> value2 = (Map<String, String>) rs.getObject(2);
                assertEquals(value2.get("key"), "this_is_key");
                Map<String, String> value3 = (Map<String, String>) rs.getObject(3);
                assertEquals(value3.get("key"), "a_key");
                Map<String, LocalDate> value4 = (Map<String, LocalDate>) rs.getObject(4);
                assertEquals(Date.valueOf(value4.get("key")), java.sql.Date.valueOf("1970-01-01"));
                Map<String, Object> value5 = (Map<String, Object>) rs.getObject(5);
                assertEquals(DateTimeUtil.toTimestamp((ZonedDateTime) value5.get("key"), null), java.sql.Timestamp.valueOf("2021-01-01 00:00:00"));
                Map<String, BigDecimal> value6 = (Map<String, BigDecimal>) rs.getObject(6);
                assertEquals(value6.get("key"), BigDecimal.valueOf(412341.21D).setScale(3, RoundingMode.HALF_UP));
                Map<String, BigDecimal> value7 = (Map<String, BigDecimal>) rs.getObject(7);
                assertEquals(value7.get("key"), BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP));
                Map<String, BigDecimal> value8 = (Map<String, BigDecimal>) rs.getObject(8);
                assertEquals(value8.get("key"), BigDecimal.valueOf(412341.21D).setScale(16, RoundingMode.HALF_UP));
                Map<String, BigDecimal> value9 = (Map<String, BigDecimal>) rs.getObject(9);
                assertEquals(value9.get("key"), BigDecimal.valueOf(412341.21D).setScale(26, RoundingMode.HALF_UP));
                Map<String, String> value10 = (Map<String, String>) rs.getObject(10);
                assertEquals(value10.get("key"), "test");
                Map<String, String> value11 = (Map<String, String>) rs.getObject(11);
                assertEquals(value11.get("key"), "test");
                Map<String, Float> value12 = (Map<String, Float>) rs.getObject(12);
                assertTrue(Math.abs(value12.get("key")-1.1f)<1e-9);
                Map<String, Double> value13 = (Map<String, Double>) rs.getObject(13);
                assertTrue(Math.abs(value13.get("key")-1.1)<1e-9);
                Map<String, Long> value14 = (Map<String, Long>) rs.getObject(14);
                assertEquals(value14.get("key"), testIPv4Value1);
                Map<String, BigInteger> value15 = (Map<String, BigInteger>) rs.getObject(15);
                assertEquals(value15.get("key"), testIPv6Value1);
                Map<String, Boolean> value16 = (Map<String, Boolean>) rs.getObject(16);
                assertEquals(value16.get("key"), (byte) 1);
                Map<String, BigInteger> value17 = (Map<String, BigInteger>) rs.getObject(17);
                assertEquals(value17.get("key"), uuid);

            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS map_test");
        });
    }

    @Test
    public void testMapType_AllKey() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS map_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS map_test (value_int8 map(int8, int8),"
                    +"values map(string, string), "
                    +"valuefs map(fixed_string(5), fixed_string(5)), "
                    +"value_enum map(enum8('test','test2'), enum8('test','test2')), "
                    +"value_enum16 map(enum16('test','test2'), enum16('test','test2')), "
                    +"value_ipv4 map(ipv4, ipv4), value_ipv6 map(ipv4, ipv6), value_int16 map(int16, int16), "
                    +"value_int32 map(int32, int32), value_int64 map(int64, int64),"
                    +"value_int128 map(int128, int128), value_int256 map(int256, int256),"
                    +"value_uint8 map(uint8, uint8), value_uint16 map(uint16, uint16),"
                    +"value_uint32 map(uint32, uint32), value_uint64 map(uint64, uint64),"
                    +"value_uint128 map(uint128, uint128), value_uint256 map(uint256, uint256), "
                    +"value_boolean map(boolean, boolean), "
                    +"value_uuid map(uuid, uuid)) Engine=Memory()");

            Integer rowCnt = 100;
            Long testIPv4Value1 = ipToLong("192.168.1.1");
            BigInteger testIPv6Value1 = new BigInteger("20010db885a3000000008a2e03707334", 16);
            java.util.UUID uuid = java.util.UUID.randomUUID();
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO map_test (value_int8, values, valuefs, value_enum, value_enum16, value_ipv4, value_ipv6, value_int16, value_int32, value_int64, value_int128, value_int256, "
                    +"value_uint8, value_uint16, value_uint32, value_uint64, value_uint128, value_uint256, value_boolean, value_uuid) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")) {
                
                Map<String, String> maps = new HashMap<>();
                maps.put("this_is_key", "this_is_key");
                Map<String, String> mapfs = new HashMap<>();
                mapfs.put("a_key", "a_key");
                Map<String, String> map_enum = new HashMap<>();
                map_enum.put("test", "test");
                Map<Long, Long> map_ipv4 = new HashMap<>();
                map_ipv4.put(testIPv4Value1, testIPv4Value1);
                Map<Long, BigInteger> map_ipv6 = new HashMap<>();
                map_ipv6.put(testIPv4Value1, testIPv6Value1);
                Map<Byte, Byte> map_int8 = new HashMap<>();
                map_int8.put((byte) 1, (byte) 1);
                Map<Short, Short> map_int16 = new HashMap<>();
                map_int16.put((short) 1, (short) 1);
                Map<Integer, Integer> map_int32 = new HashMap<>();
                map_int32.put(1, 1);
                Map<Long, Long> map_int64 = new HashMap<>();
                map_int64.put(1L, 1L);
                Map<BigInteger, BigInteger> map_int128 = new HashMap<>();
                map_int128.put(testIPv6Value1, testIPv6Value1);
                Map<java.util.UUID, java.util.UUID> map_uuid = new HashMap<>();
                map_uuid.put(uuid, uuid);

                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setObject(1, map_int8);
                    pstmt.setObject(2, maps);
                    pstmt.setObject(3, mapfs);
                    pstmt.setObject(4, map_enum);
                    pstmt.setObject(5, map_enum);
                    pstmt.setObject(6, map_ipv4);
                    pstmt.setObject(7, map_ipv6);
                    pstmt.setObject(8, map_int16);
                    pstmt.setObject(9, map_int32);
                    pstmt.setObject(10, map_int64);
                    pstmt.setObject(11, map_int128);
                    pstmt.setObject(12, map_int128);
                    pstmt.setObject(13, map_int16);
                    pstmt.setObject(14, map_int32);
                    pstmt.setObject(15, map_int64);
                    pstmt.setObject(16, map_int64);
                    pstmt.setObject(17, map_int128);
                    pstmt.setObject(18, map_int128);
                    pstmt.setObject(19, map_int8);
                    pstmt.setObject(20, map_uuid);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM map_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                // check the value
                Map<Byte, Byte> value = (Map<Byte, Byte>) rs.getObject(1);
                assertEquals(value.get((byte) 1), (byte) 1);
                Map<String, String> value2 = (Map<String, String>) rs.getObject(2);
                assertEquals(value2.get("this_is_key"), "this_is_key");
                Map<String, String> value3 = (Map<String, String>) rs.getObject(3);
                assertEquals(value3.get("a_key"), "a_key");
                Map<String, String> value4 = (Map<String, String>) rs.getObject(4);
                assertEquals(value4.get("test"), "test");
                Map<String, String> value5 = (Map<String, String>) rs.getObject(5);
                assertEquals(value5.get("test"), "test");
                Map<Long, Long> value6 = (Map<Long, Long>) rs.getObject(6);
                assertEquals(value6.get(testIPv4Value1), testIPv4Value1);
                Map<Long, BigInteger> value7 = (Map<Long, BigInteger>) rs.getObject(7);
                assertEquals(value7.get(testIPv4Value1), testIPv6Value1);
                Map<Short, Short> value8 = (Map<Short, Short>) rs.getObject(8);
                assertEquals(value8.get((short) 1), (short) 1);
                Map<Integer, Integer> value9 = (Map<Integer, Integer>) rs.getObject(9);
                assertEquals(value9.get(1), 1);
                Map<Long, Long> value10 = (Map<Long, Long>) rs.getObject(10);
                assertEquals(value10.get(1l), 1);
                Map<BigInteger, BigInteger> value11 = (Map<BigInteger, BigInteger>) rs.getObject(11);
                assertEquals(value11.get(testIPv6Value1), testIPv6Value1);
                Map<BigInteger, BigInteger> value12 = (Map<BigInteger, BigInteger>) rs.getObject(12);
                assertEquals(value12.get(testIPv6Value1), testIPv6Value1);
                Map<Short, Short> value13 = (Map<Short, Short>) rs.getObject(13);
                assertEquals(value13.get((short) 1), (short) 1);
                Map<Integer, Integer> value14 = (Map<Integer, Integer>) rs.getObject(14);
                assertEquals(value14.get(1), 1);
                Map<Long, Long> value15 = (Map<Long, Long>) rs.getObject(15);
                assertEquals(value15.get(1l), 1);
                Map<BigInteger, BigInteger> value16 = (Map<BigInteger, BigInteger>) rs.getObject(16);
                assertEquals(value16.get(new BigInteger("1")), new BigInteger("1"));
                Map<BigInteger, BigInteger> value17 = (Map<BigInteger, BigInteger>) rs.getObject(17);
                assertEquals(value17.get(testIPv6Value1), testIPv6Value1);
                Map<BigInteger, BigInteger> value18 = (Map<BigInteger, BigInteger>) rs.getObject(18);
                assertEquals(value18.get(testIPv6Value1), testIPv6Value1);
                Map<Byte, Byte> value19 = (Map<Byte, Byte>) rs.getObject(19);
                assertEquals(value19.get((byte) 1), (byte) 1);
                Map<java.util.UUID, java.util.UUID> value20 = (Map<java.util.UUID, java.util.UUID>) rs.getObject(20);
                assertEquals(value20.get(uuid), uuid);
            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS map_test");
        });
    }

    @Test
    public void testMapType_NullValue() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS map_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS map_test (value map(string, nullable(int)),"
                    +"values map(string, nullable(string)), "
                    +"valuefs map(string, nullable(fixed_string(5))), "
                    +"value_date map(string, nullable(date)), "
                    +"value_datetime map(string, nullable(datetime)), "
                    +"value_decimal map(string, nullable(decimal(15, 5))), "
                    +"value_enum map(string, nullable(enum8('test','test2'))), "
                    +"value_float map(string, nullable(float32)), "
                    +"value_ipv4 map(string, nullable(ipv4)), "
                    +"value_ipv6 map(string, nullable(ipv6)), "
                    +"value_boolean map(string, nullable(boolean)), "
                    +"value_uuid map(string, nullable(uuid))) Engine=Memory()");

            Integer rowCnt = 100;
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO map_test (value, values, valuefs, value_date, value_datetime, value_decimal, value_enum, value_float, value_ipv4, value_ipv6, value_boolean, value_uuid) values(?,?,?,?,?,?,?,?,?,?,?,?);")) {

                Map<String, Integer> map = new HashMap<>();
                map.put("key", null);

                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setObject(1, map);
                    pstmt.setObject(2, map);
                    pstmt.setObject(3, map);
                    pstmt.setObject(4, map);
                    pstmt.setObject(5, map);
                    pstmt.setObject(6, map);
                    pstmt.setObject(7, map);
                    pstmt.setObject(8, map);
                    pstmt.setObject(9, map);
                    pstmt.setObject(10, map);
                    pstmt.setObject(11, map);
                    pstmt.setObject(12, map);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM map_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                // check the value
                Map<String, Integer> value = (Map<String, Integer>) rs.getObject(1);
                assertEquals(value.get("key"), null);
                Map<String, String> value2 = (Map<String, String>) rs.getObject(2);
                assertEquals(value2.get("key"), null);
                Map<String, String> value3 = (Map<String, String>) rs.getObject(3);
                assertEquals(value3.get("key"), null);
                Map<String, Date> value4 = (Map<String, Date>) rs.getObject(4);
                assertEquals(value4.get("key"), null);
                Map<String, Object> value5 = (Map<String, Object>) rs.getObject(5);
                assertEquals(value5.get("key"), null);
                Map<String, BigDecimal> value6 = (Map<String, BigDecimal>) rs.getObject(6);
                assertEquals(value6.get("key"), null);
                Map<String, String> value7 = (Map<String, String>) rs.getObject(7);
                assertEquals(value7.get("key"), null);
                Map<String, Float> value8 = (Map<String, Float>) rs.getObject(8);
                assertEquals(value8.get("key"), null);
                Map<String, Long> value9 = (Map<String, Long>) rs.getObject(9);
                assertEquals(value9.get("key"), null);
                Map<String, BigInteger> value10 = (Map<String, BigInteger>) rs.getObject(10);
                assertEquals(value10.get("key"), null);
                Map<String, Byte> value11 = (Map<String, Byte>) rs.getObject(11);
                assertEquals(value11.get("key"), null);
                Map<String, java.util.UUID> value12 = (Map<String, java.util.UUID>) rs.getObject(12);
                assertEquals(value12.get("key"), null);

            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS map_test");
        });
    }

    @Test
    public void testMapType_MultipleScenerio() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS map_test");
            statement.execute(
                    "CREATE STREAM IF NOT EXISTS map_test (value map(string, nullable(int)),"
                    +"values map(string, nullable(string)), "
                    +"valuefs map(string, nullable(fixed_string(5))), "
                    +"value_date map(int, nullable(date)), "
                    +"value_datetime map(int, nullable(datetime)), "
                    +"value_decimal map(int256, nullable(decimal(15, 5))), "
                    +"value_enum map(string, nullable(enum8('test','test2'))), "
                    +"value_float map(uint256, nullable(float32)), "
                    +"value_ipv4 map(ipv4, nullable(ipv4)), "
                    +"value_ipv6 map(uint256, nullable(ipv6)), "
                    +"value_boolean map(boolean, nullable(boolean)), "
                    +"value_uuid map(uuid, nullable(uuid))) Engine=Memory()");

            Integer rowCnt = 100;
            Long testIPv4Value1 = ipToLong("192.168.1.1");
            BigInteger testIPv6Value1 = new BigInteger("20010db885a3000000008a2e03707334", 16);
            java.util.UUID uuid = java.util.UUID.randomUUID();
            java.util.UUID uuid2 = java.util.UUID.randomUUID();
            java.util.UUID uuid3 = java.util.UUID.randomUUID();
            try (PreparedStatement pstmt = statement.getConnection().prepareStatement(
                    "INSERT INTO map_test (value, values, valuefs, value_date, value_datetime, value_decimal, value_enum, value_float, value_ipv4, value_ipv6, value_boolean, value_uuid) values(?,?,?,?,?,?,?,?,?,?,?,?);")) {

                Map<String, Integer> map = new HashMap<>();
                map.put("key", null);
                map.put("key2", 1);
                map.put("key3", null);
                Map<String, String> maps = new HashMap<>();
                maps.put("key", null);
                maps.put("key2", "this_is_key");
                maps.put("key3", null);
                Map<String, String> mapfs = new HashMap<>();
                mapfs.put("key", null);
                mapfs.put("key2", "a_key");
                mapfs.put("key3", null);
                Map<Integer, Date> map_date = new HashMap<>();
                map_date.put(1, null);
                map_date.put(2, java.sql.Date.valueOf("1970-01-01"));
                map_date.put(3, null);
                Map<Integer, java.sql.Timestamp> map_datetime = new HashMap<>();
                map_datetime.put(1, null);
                map_datetime.put(2, java.sql.Timestamp.valueOf("2021-01-01 00:00:00"));
                map_datetime.put(3, null);
                Map<BigInteger, BigDecimal> map_decimal = new HashMap<>();
                map_decimal.put(BigInteger.valueOf(1), null);
                map_decimal.put(BigInteger.valueOf(2), BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP));
                map_decimal.put(BigInteger.valueOf(3), null);
                Map<String, String> map_enum = new HashMap<>();
                map_enum.put("key", null);
                map_enum.put("key2", "test");
                map_enum.put("key3", null);
                Map<BigInteger, Float> map_float = new HashMap<>();
                map_float.put(BigInteger.valueOf(1), null);
                map_float.put(BigInteger.valueOf(2), 1.1f);
                map_float.put(BigInteger.valueOf(3), null);
                Map<Long, Long> map_ipv4 = new HashMap<>();
                map_ipv4.put(1l, null);
                map_ipv4.put(2l, testIPv4Value1);
                map_ipv4.put(3l, null);
                Map<BigInteger, BigInteger> map_ipv6 = new HashMap<>();
                map_ipv6.put(BigInteger.valueOf(1), null);
                map_ipv6.put(BigInteger.valueOf(2), testIPv6Value1);
                map_ipv6.put(BigInteger.valueOf(3), null);
                Map<Byte, Byte> map_bool = new HashMap<>();
                map_bool.put((byte) 1, null);
                map_bool.put((byte) 0, (byte) 1);
                Map<java.util.UUID, java.util.UUID> map_uuid = new HashMap<>();
                map_uuid.put(uuid, null);
                map_uuid.put(uuid2, uuid);
                map_uuid.put(uuid3, null);


                for (int i = 0; i < rowCnt; i++) {
                    pstmt.setObject(1, map);
                    pstmt.setObject(2, maps);
                    pstmt.setObject(3, mapfs);
                    pstmt.setObject(4, map_date);
                    pstmt.setObject(5, map_datetime);
                    pstmt.setObject(6, map_decimal);
                    pstmt.setObject(7, map_enum);
                    pstmt.setObject(8, map_float);
                    pstmt.setObject(9, map_ipv4);
                    pstmt.setObject(10, map_ipv6);
                    pstmt.setObject(11, map_bool);
                    pstmt.setObject(12, map_uuid);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            ResultSet rs = statement.executeQuery("SELECT * FROM map_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                // check the value
                Map<String, Integer> value = (Map<String, Integer>) rs.getObject(1);
                assertEquals(value.get("key"), null);
                assertEquals(value.get("key2"), 1);
                assertEquals(value.get("key3"), null);
                Map<String, String> value2 = (Map<String, String>) rs.getObject(2);
                assertEquals(value2.get("key"), null);
                assertEquals(value2.get("key2"), "this_is_key");
                assertEquals(value2.get("key3"), null);
                Map<String, String> value3 = (Map<String, String>) rs.getObject(3);
                assertEquals(value3.get("key"), null);
                assertEquals(value3.get("key2"), "a_key");
                assertEquals(value3.get("key3"), null);
                Map<Integer, LocalDate> value4 = (Map<Integer, LocalDate>) rs.getObject(4);
                assertEquals(value4.get(1), null);
                assertEquals(Date.valueOf(value4.get(2)), java.sql.Date.valueOf("1970-01-01"));
                assertEquals(value4.get(3), null);
                Map<Integer, Object> value5 = (Map<Integer, Object>) rs.getObject(5);
                assertEquals(value5.get(1), null);
                assertEquals(DateTimeUtil.toTimestamp((ZonedDateTime) value5.get(2), null), java.sql.Timestamp.valueOf("2021-01-01 00:00:00"));
                assertEquals(value5.get(3), null);
                Map<BigInteger, BigDecimal> value6 = (Map<BigInteger, BigDecimal>) rs.getObject(6);
                assertEquals(value6.get(BigInteger.valueOf(1)), null);
                assertEquals(value6.get(BigInteger.valueOf(2)), BigDecimal.valueOf(412341.21D).setScale(5, RoundingMode.HALF_UP));
                assertEquals(value6.get(BigInteger.valueOf(3)), null);
                Map<String, String> value7 = (Map<String, String>) rs.getObject(7);
                assertEquals(value7.get("key"), null);
                assertEquals(value7.get("key2"), "test");
                assertEquals(value7.get("key3"), null);
                Map<BigInteger, Float> value8 = (Map<BigInteger, Float>) rs.getObject(8);
                assertEquals(value8.get(BigInteger.valueOf(1)), null);
                assertEquals(value8.get(BigInteger.valueOf(2)), 1.1f);
                assertEquals(value8.get(BigInteger.valueOf(3)), null);
                Map<Long, Long> value9 = (Map<Long, Long>) rs.getObject(9);
                assertEquals(value9.get(1l), null);
                assertEquals(value9.get(2l), testIPv4Value1);
                assertEquals(value9.get(3l), null);
                Map<BigInteger, BigInteger> value10 = (Map<BigInteger, BigInteger>) rs.getObject(10);
                assertEquals(value10.get(BigInteger.valueOf(1)), null);
                assertEquals(value10.get(BigInteger.valueOf(2)), testIPv6Value1);
                assertEquals(value10.get(BigInteger.valueOf(3)), null);
                Map<Byte, Byte> value11 = (Map<Byte, Byte>) rs.getObject(11);
                assertEquals(value11.get((byte) 1), null);
                assertEquals(value11.get((byte) 0), (byte) 1);
                Map<java.util.UUID, java.util.UUID> value12 = (Map<java.util.UUID, java.util.UUID>) rs.getObject(12);
                assertEquals(value12.get(uuid), null);
                assertEquals(value12.get(uuid2), uuid);
                assertEquals(value12.get(uuid3), null);

            }

            assertEquals(size, rowCnt);

            statement.execute("DROP STREAM IF EXISTS map_test");
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
