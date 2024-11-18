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

package com.timeplus.protocol;

import com.timeplus.client.NativeContext;
import com.timeplus.data.Block;
import com.timeplus.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

public class TotalsResponse implements Response {

    public static TotalsResponse readFrom(BinaryDeserializer deserializer, NativeContext.ServerContext info)
            throws IOException, SQLException {
        String name = deserializer.readUTF8StringBinary(); /// external table name

        deserializer.maybeEnableCompressed();
        Block block = Block.readFrom(deserializer, info);
        deserializer.maybeDisableCompressed();
        return new TotalsResponse(name, block);
    }

    private final String name;
    private final Block block;

    TotalsResponse(String name, Block block) {
        this.name = name;
        this.block = block;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_TOTALS;
    }

    public String name() {
        return name;
    }

    public Block block() {
        return block;
    }
}
