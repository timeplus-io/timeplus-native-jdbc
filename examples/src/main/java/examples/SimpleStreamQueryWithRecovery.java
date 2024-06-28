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

package examples;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class SimpleStreamQueryWithRecovery {
    final String ckpt_file = "query.ckpt";
    final RandomAccessFile writer;

    SimpleStreamQueryWithRecovery() throws IOException {
        writer = new RandomAccessFile(ckpt_file, "rw");
    }

    void checkpoint(long last_consumed_sn) throws IOException {
        writer.seek(0);
        writer.writeLong(last_consumed_sn);
    }

    long recover() throws IOException {
        try (RandomAccessFile reader = new RandomAccessFile(ckpt_file, "r")) {
            if (reader.length() > 0)
                return reader.readLong() + 1; /// Add one to move to next sn, since checkpoint records what we have already consumed

            /// No ckpt was written, return earliest sn
            return 1;
        }
    }

    void run() throws Exception {
        long sn = recover();
        String query = String.format("SELECT *, _tp_sn FROM test settings seek_to='%d'", sn);

        System.out.printf("Start consuming from sn=%d\n", sn);

        try (Connection connection = DriverManager.getConnection("jdbc:timeplus://127.0.0.1:8463?client_name=timeplus-example")) {
            try (Statement stmt = connection.createStatement()) {
                stmt.executeQuery("create stream if not exists test(i int, s string)");
                try (ResultSet rs = stmt.executeQuery(query)) {
                    while (rs.next()) {
                        System.out.println(rs.getInt(1) + "\t" + rs.getString(2));
                        checkpoint(rs.getLong(4));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        SimpleStreamQueryWithRecovery recover_example = new SimpleStreamQueryWithRecovery();
        recover_example.run();
    }
}
