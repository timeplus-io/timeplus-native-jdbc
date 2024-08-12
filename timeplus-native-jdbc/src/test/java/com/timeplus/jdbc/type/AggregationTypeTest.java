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
import org.junit.jupiter.api.Test;
import java.sql.ResultSet;
import java.util.Arrays;
import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AggregationTypeTest extends AbstractITest implements BytesHelper  {
    // create stream data_02295 (
    //     -- the order of "a" and "b" is important here
    //     -- (since finalizeChunk() accepts positions and they may be wrong)
    //     b int64,
    //     a int64,
    //     grp_aggreg aggregate_function(group_array_array, array(uint64))
    // ) engine = MergeTree() order by a;
    // insert into data_02295 select 0 as b, int_div(number, 2) as a, group_array_array_state([to_uint64(number)]) from numbers(4) group by a, b;
    // SELECT grp_aggreg FROM data_02295 GROUP BY a, grp_aggreg WITH TOTALS ORDER BY a SETTINGS optimize_aggregation_in_order = 0 FORMAT JSONEachRow;
    
    @Test
    public void testType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS agg_test");
            statement.execute(
                    "CREATE STREAM agg_test (" +
                    "b int64,"+
                    "a int64, "+ 
                    "grp_aggreg aggregate_function(group_array_array, array(uint64))"+
                    ") engine = MergeTree() order by a;");

            statement.executeQuery("INSERT INTO agg_test select 0 as b, int_div(number, 2) as a, group_array_array_state([to_uint64(number)]) from numbers(4) group by a, b;");

            ResultSet rs = statement.executeQuery("SELECT grp_aggreg FROM agg_test GROUP BY a, grp_aggreg WITH TOTALS ORDER BY a SETTINGS optimize_aggregation_in_order = 0;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object obj = rs.getObject(1);
                assertNotNull(obj);
            }
            assertEquals(2, size);

            rs = statement.executeQuery("SELECT a, min(b), max(b) FROM agg_test GROUP BY a ORDER BY a, count() SETTINGS optimize_aggregation_in_order = 1, max_threads = 1;");
            size = 0;
            while (rs.next()) {
                size++;
                Object obj = rs.getObject(1);
                assertNotNull(obj);
            }
            assertEquals(2, size);
            statement.execute("DROP STREAM IF EXISTS agg_test");
        });
    }

    @Test
    public void testComplexType() throws Exception {
        withStatement(statement -> {
            statement.execute("DROP STREAM IF EXISTS agg_test");
            statement.execute(
                    "CREATE STREAM agg_test (" +
                    "a int64, grp_aggreg aggregate_function(group_array_array, array(uint64)),"+
                    "grp_simple simple_aggregate_function(group_array_array, array(uint64))) engine = MergeTree() order by a");

            statement.executeQuery("INSERT INTO agg_test select 1 as a, group_array_array_state([to_uint64(number)]), group_array_array([to_uint64(number)]) from numbers(2) group by a;");

            ResultSet rs = statement.executeQuery("SELECT array_sort(group_array_array_merge(grp_aggreg)) as gra , array_sort(group_array_array(grp_simple)) as grs FROM agg_test group by a SETTINGS optimize_aggregation_in_order=1;");
            int size = 0;
            while (rs.next()) {
                size++;
                Object[] objArray = (Object[]) rs.getArray(1).getArray();
                BigInteger[] value = Arrays.stream(objArray).map(BigInteger.class::cast).toArray(BigInteger[]::new);
                assertTrue(Arrays.equals(value, new BigInteger[]{new BigInteger("0"), new BigInteger("1")}));
            }
            assertEquals(1, size);
            statement.execute("DROP STREAM IF EXISTS agg_test");
        });
    }
}
