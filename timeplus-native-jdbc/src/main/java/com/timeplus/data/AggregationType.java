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

package com.timeplus.data;

public enum AggregationType {
    numbers(0),
    group_array_array(1),
    int_div(2),
    to_uint64(3),
    min(4),
    max(5),
    sum(6),
    array_sort(7),
    count(8),
    median(9),
    avg(10),
    row_number(11),
    //aggregate function conbinators
    distinct_streaming(100),
    distinct_retract(101),
    simple_state(102),
    or_default(103),
    distinct(104),
    resample(105),
    for_each(106),
    or_null(107),
    merge(108),
    state(109),
    array(110),
    map(111);

    private final int value;

    AggregationType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static boolean isValidType(String type) {
        try {
            AggregationType.valueOf(type);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
