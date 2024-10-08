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

import com.timeplus.serde.BinaryDeserializer;

import java.io.IOException;
import java.sql.SQLException;

public class PongResponse implements Response {

    public static final PongResponse INSTANCE = new PongResponse();

    public static PongResponse readFrom(BinaryDeserializer deserializer) throws IOException, SQLException {
        return INSTANCE;
    }

    @Override
    public ProtoType type() {
        return ProtoType.RESPONSE_PONG;
    }
}
