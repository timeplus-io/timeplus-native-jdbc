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

public enum IndexType {
    UInt8(0),
    UInt16(1),
    UInt32(2),
    UInt64(3),
    NeedGlobalDictionaryBit(1 << 8),
    // Need to read additional keys. Additional keys are stored before indexes as value N and N keys after them.
    HasAdditionalKeysBit(1 << 9),
    // Need to update dictionary. It means that previous granule has different dictionary.
    NeedUpdateDictionary(1 << 10);

    private final int value;

    IndexType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
