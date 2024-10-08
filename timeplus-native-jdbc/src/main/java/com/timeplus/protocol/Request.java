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

import com.timeplus.serde.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public interface Request {

    ProtoType type();

    void writeImpl(BinarySerializer serializer) throws IOException, SQLException;

    default void writeTo(BinarySerializer serializer) throws IOException, SQLException {
        serializer.writeVarInt(type().id());
        this.writeImpl(serializer);
    }

    enum ProtoType {
        REQUEST_HELLO(0),
        REQUEST_QUERY(1),
        REQUEST_DATA(2),
        REQUEST_PING(4);

        private final int id;

        ProtoType(int id) {
            this.id = id;
        }

        public long id() {
            return id;
        }
    }
}
