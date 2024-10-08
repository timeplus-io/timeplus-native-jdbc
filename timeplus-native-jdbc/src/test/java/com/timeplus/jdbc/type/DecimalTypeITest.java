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
import com.timeplus.jdbc.TimeplusArray;
import com.timeplus.misc.BytesHelper;
import com.google.common.base.Strings;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DecimalTypeITest extends AbstractITest implements BytesHelper {

    @Test
    public void testDecimalType() throws Exception {
        withStatement(statement -> {
            BigDecimal value32 = BigDecimal.valueOf(1.32);
            value32 = value32.setScale(2, RoundingMode.HALF_UP);
            BigDecimal value64 = new BigDecimal("12343143412341.21");
            value64 = value64.setScale(5, RoundingMode.HALF_UP);

            BigDecimal value128 = new BigDecimal(Strings.repeat("1", (38 - 16)));
            value128 = value128.setScale(16, RoundingMode.HALF_UP);

            BigDecimal value256 = new BigDecimal(Strings.repeat("1", (76 - 26)));
            value256 = value256.setScale(26, RoundingMode.HALF_UP);

            BigDecimal value256Neg = new BigDecimal("-1" + Strings.repeat("1", (76 - 26)));
            value256Neg = value256Neg.setScale(26, RoundingMode.HALF_UP);

            BigDecimal[] valueArray = new BigDecimal[]{
                    BigDecimal.valueOf(412341.21D).setScale(3, RoundingMode.HALF_UP),
                    BigDecimal.valueOf(512341.25D).setScale(3, RoundingMode.HALF_UP)
            };

            statement.execute("DROP STREAM IF EXISTS decimal_test");
            statement.execute("CREATE STREAM IF NOT EXISTS decimal_test (value32 Decimal(7,2), "
                    + "value64 decimal(15,5), "
                    + "value128 decimal(38, 16),"
                    + "value256 decimal(76, 26),"
                    + "value256_neg decimal(76, 26),"
                    + "value_array array(decimal(5,3))) Engine=Memory()");

            try (PreparedStatement pstmt = statement.getConnection().prepareStatement("INSERT INTO decimal_test"
                    + "(value32,value64,value128,value256,value256_neg,value_array) "
                    + "values(?,?,?,?,?,?);")) {
                for (int i = 0; i < 300; i++) {
                    pstmt.setBigDecimal(1, value32);
                    pstmt.setBigDecimal(2, value64);
                    pstmt.setBigDecimal(3, value128);
                    pstmt.setBigDecimal(4, value256);
                    pstmt.setBigDecimal(5, value256Neg);
                    pstmt.setArray(6, pstmt.getConnection().createArrayOf("Decimal(5,3)", valueArray));
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            // Check data count
            ResultSet rs = statement.executeQuery("SELECT * FROM decimal_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigDecimal rsValue32 = rs.getBigDecimal(1);
                assertEquals(value32, rsValue32);
                BigDecimal rsValue64 = rs.getBigDecimal(2);
                assertEquals(value64, rsValue64);
                BigDecimal rsValue128 = rs.getBigDecimal(3);
                assertEquals(value128, rsValue128);
                BigDecimal rsValue256 = rs.getBigDecimal(4);
                assertEquals(value256, rsValue256);
                BigDecimal rsValue256Neg = rs.getBigDecimal(5);
                assertEquals(value256Neg, rsValue256Neg);

                TimeplusArray rsValueArray = (TimeplusArray) rs.getArray(6);
                Object[] decimalArray = rsValueArray.getArray();
                assertEquals(decimalArray.length, valueArray.length);
                for (int i = 0; i < valueArray.length; i++) {
                    assertEquals(decimalArray[i], valueArray[i]);
                }
            }
            assertEquals(300, size);
            statement.execute("DROP STREAM IF EXISTS decimal_test");
        }, "allow_experimental_bigint_types", 1);
    }


    @Test
    public void testNegativeDecimal() {
        int scale = 4;
        BigDecimal negative = new BigDecimal("-18.2000", MathContext.DECIMAL128);
        BigDecimal scaleFactor = BigDecimal.valueOf(Math.pow(10, scale));
        BigDecimal targetValue = negative.multiply(scaleFactor);
        BigInteger res = targetValue.toBigInteger();

        long l1 = res.longValue();
        long l2 = res.shiftRight(64).longValue();

        // v =  -1820000
        long[] arr = new long[]{l2, l1};
        BigInteger v = new BigInteger(getBytes(arr));
        BigDecimal value = new BigDecimal(v);
        value = value.divide(scaleFactor, scale, RoundingMode.HALF_UP);

        assertEquals(negative, value);
    }

    @Test
    public void testDecimal32Type() throws Exception {
        withStatement(statement -> {
            BigDecimal value32 = BigDecimal.valueOf(1.32);
            value32 = value32.setScale(2, RoundingMode.HALF_UP);

            statement.execute("DROP STREAM IF EXISTS decimal32_test");
            statement.execute("CREATE STREAM IF NOT EXISTS decimal32_test (value32 decimal32(2)) Engine=Memory()");

            try (PreparedStatement pstmt = statement.getConnection().prepareStatement("INSERT INTO decimal32_test"
                    + "(value32) "
                    + "values(?);")) {
                for (int i = 0; i < 300; i++) {
                    pstmt.setBigDecimal(1, value32);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            ResultSet rs = statement.executeQuery("SELECT * FROM decimal32_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigDecimal rsValue32 = rs.getBigDecimal(1);
                assertEquals(value32, rsValue32);
            }
            assertEquals(300, size);

            statement.execute("DROP STREAM IF EXISTS decimal32_test");
        });
    }

    @Test
    public void testDecimal64Type() throws Exception {
        withStatement(statement -> {
            BigDecimal value64 = new BigDecimal("12343143412341.21");
            value64 = value64.setScale(5, RoundingMode.HALF_UP);

            statement.execute("DROP STREAM IF EXISTS decimal64_test");
            statement.execute("CREATE STREAM IF NOT EXISTS decimal64_test (value64 decimal64(5)) Engine=Memory()");

            try (PreparedStatement pstmt = statement.getConnection().prepareStatement("INSERT INTO decimal64_test"
                    + "(value64) "
                    + "values(?);")) {
                for (int i = 0; i < 300; i++) {
                    pstmt.setBigDecimal(1, value64);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            ResultSet rs = statement.executeQuery("SELECT * FROM decimal64_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigDecimal rsValue64 = rs.getBigDecimal(1);
                assertEquals(value64, rsValue64);
            }
            assertEquals(300, size);

            statement.execute("DROP STREAM IF EXISTS decimal64_test");
        }, "allow_experimental_bigint_types", 1);
    }

    @Test
    public void testDecimal128Type() throws Exception {
        withStatement(statement -> {
            BigDecimal value128 = new BigDecimal(Strings.repeat("1", (38 - 16)));
            value128 = value128.setScale(16, RoundingMode.HALF_UP);

            statement.execute("DROP STREAM IF EXISTS decimal128_test");
            statement.execute("CREATE STREAM IF NOT EXISTS decimal128_test (value128 decimal128(16)) Engine=Memory()");

            try (PreparedStatement pstmt = statement.getConnection().prepareStatement("INSERT INTO decimal128_test"
                    + "(value128) "
                    + "values(?);")) {
                for (int i = 0; i < 300; i++) {
                    pstmt.setBigDecimal(1, value128);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            ResultSet rs = statement.executeQuery("SELECT * FROM decimal128_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigDecimal rsValue128 = rs.getBigDecimal(1);
                assertEquals(value128, rsValue128);
            }
            assertEquals(300, size);

            statement.execute("DROP STREAM IF EXISTS decimal128_test");
        }, "allow_experimental_bigint_types", 1);
    }

    @Test
    public void testDecimal256Type() throws Exception {
        withStatement(statement -> {
            BigDecimal value256 = new BigDecimal(Strings.repeat("1", (76 - 26)));
            value256 = value256.setScale(26, RoundingMode.HALF_UP);


            statement.execute("DROP STREAM IF EXISTS decimal256_test");
            statement.execute("CREATE STREAM IF NOT EXISTS decimal256_test (value256 decimal256(26)) Engine=Memory()");

            try (PreparedStatement pstmt = statement.getConnection().prepareStatement("INSERT INTO decimal256_test"
                    + "(value256) "
                    + "values(?);")) {
                for (int i = 0; i < 300; i++) {
                    pstmt.setBigDecimal(1, value256);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            ResultSet rs = statement.executeQuery("SELECT * FROM decimal256_test;");
            int size = 0;
            while (rs.next()) {
                size++;
                BigDecimal rsValue256 = rs.getBigDecimal(1);
                assertEquals(value256, rsValue256);
            }
            assertEquals(300, size);

            statement.execute("DROP STREAM IF EXISTS decimal256_test");
        }, "allow_experimental_bigint_types", 1);
    }
    
}
