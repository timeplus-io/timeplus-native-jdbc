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

package com.timeplus.client;

import com.timeplus.serde.BinarySerializer;
import com.timeplus.settings.TimeplusConfig;
import com.timeplus.settings.TimeplusDefines;

import java.io.IOException;
import java.time.ZoneId;

public class NativeContext {

    private final ClientContext clientCtx;
    private final ServerContext serverCtx;
    private final NativeClient nativeClient;

    public NativeContext(ClientContext clientCtx, ServerContext serverCtx, NativeClient nativeClient) {
        this.clientCtx = clientCtx;
        this.serverCtx = serverCtx;
        this.nativeClient = nativeClient;
    }

    public ClientContext clientCtx() {
        return clientCtx;
    }

    public ServerContext serverCtx() {
        return serverCtx;
    }

    public NativeClient nativeClient() {
        return nativeClient;
    }

    public static class ClientContext {
        public static final int TCP_KINE = 1;

        public static final byte NO_QUERY = 0;
        public static final byte INITIAL_QUERY = 1;
        public static final byte SECONDARY_QUERY = 2;

        private final String clientName;
        private final String clientHostname;
        private final String initialAddress;

        public ClientContext(String initialAddress, String clientHostname, String clientName) {
            this.clientName = clientName;
            this.clientHostname = clientHostname;
            this.initialAddress = initialAddress;
        }

        public void writeTo(BinarySerializer serializer) throws IOException {
            /// ClientInfo
            serializer.writeByte(ClientContext.INITIAL_QUERY);
            serializer.writeUTF8StringBinary(""); /// initial user
            serializer.writeUTF8StringBinary(""); /// initial query_id
            serializer.writeUTF8StringBinary(initialAddress); /// initial address

            serializer.writeLong(0); /// initial query start time microseconds

            // for TCP kind
            serializer.writeByte((byte) TCP_KINE); /// interface
            serializer.writeUTF8StringBinary(""); /// os user
            serializer.writeUTF8StringBinary(clientHostname);
            serializer.writeUTF8StringBinary(clientName);
            serializer.writeVarInt(TimeplusDefines.MAJOR_VERSION);
            serializer.writeVarInt(TimeplusDefines.MINOR_VERSION);
            serializer.writeVarInt(TimeplusDefines.CLIENT_REVISION);
            serializer.writeUTF8StringBinary(""); /// quota key
            serializer.writeVarInt(0); /// distributed depth
            serializer.writeVarInt(0); /// client version patch
            serializer.writeByte((byte) 0); /// no open telemetry

            serializer.writeVarInt(0); /// collaborate with initiator
            serializer.writeVarInt(0); /// count participating replicas
            serializer.writeVarInt(0); /// number of current replica
        }
    }

    public static class ServerContext {
        private final long majorVersion;
        private final long minorVersion;
        private final long revision;
        private final ZoneId timeZone;
        private final String displayName;
        private final TimeplusConfig configure;

        public ServerContext(long majorVersion, long minorVersion, long revision,
                             TimeplusConfig configure,
                             ZoneId timeZone, String displayName) {
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
            this.revision = revision;
            this.configure = configure;
            this.timeZone = timeZone;
            this.displayName = displayName;
        }

        public long majorVersion() {
            return majorVersion;
        }

        public long minorVersion() {
            return minorVersion;
        }

        public long revision() {
            return revision;
        }

        public String version() {
            return majorVersion + "." + minorVersion + "." + revision;
        }

        public ZoneId timeZone() {
            return timeZone;
        }

        public String displayName() {
            return displayName;
        }

        public TimeplusConfig getConfigure() {
            return configure;
        }
    }
}
