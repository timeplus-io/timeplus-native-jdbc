package examples;

import java.sql.*;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.*;

/**
 * Streaming Query
 */
public class DDosDetectionAndReport {
    private final String timeplus_jdbc_url = "jdbc:timeplus://127.0.0.1:8463";

    /// Emulate data ingestion via a random (synthesized) stream,
    /// which generates 1000 event per second when query it

    private final String ipfix_random_stream = "ipfix_random";
    private final String eps = "1000";
    private final String ipfix_random_ddl = String.format("create random stream %s (" +
            "ipv4_dst_ip uint32," +
            "protocol uint8," +
            "l4_dst_port uint16," +
            "ipv4_src_ip uint32," +
            "l4_src_port uint16," +
            "byte_count uint32 default rand() %% 10000," +
            "pkt_count uint32," +
            "start_time int32," +
            "tags string," +
            "tcp_flags uint16," +
            "input_if_id uint16," +
            "output_if_id uint16" +
            ") SETTINGS eps=%s;", ipfix_random_stream, eps);

    private final String ipfix_stream = "ipfix";
    private final String ipfix_stream_shards = "4";
    private final String ipfix_stream_flush_threshold_count = "1000";
    private final String ipfix_ddl = String.format("create stream %s (" +
            "ipv4_dst_ip uint32," +
            "protocol uint8," +
            "l4_dst_port uint16," +
            "ipv4_src_ip uint32," +
            "l4_src_port uint16," +
            "byte_count uint32," +
            "pkt_count uint32," +
            "start_time int32," +
            "tags string," +
            "tcp_flags uint16," +
            "input_if_id uint16," +
            "output_if_id uint16" +
            ") settings shards=%s, flush_threshold_count=%s", ipfix_stream, ipfix_stream_shards, ipfix_stream_flush_threshold_count);

    private final String victims_stream = "victims";
    private final String victims_stream_shards = "1";
    private final String victims_stream_flush_threshold_count = "1000";
    private final String victims_ddl = String.format("create stream %s (" +
            "victim_ip uint32," +
            "sum_bytes uint64" +
            ") settings shards=%s, flush_threshold_count=%s", victims_stream, victims_stream_shards, victims_stream_flush_threshold_count);

    private final String ddos_rule_threshold = "8000";
    private final String ddos_mv = "ddos_mv";
    private final String ddos_mv_threads = "4";
    private final String rule_threshold_detection_mv = String.format("create materialized view %s INTO %s AS " +
            "SELECT " +
            "ipv4_dst_ip AS victim_ip, " +
            "sum(byte_count) AS sum_bytes, " +
            "window_start AS _tp_time " +
            "FROM hop(ipfix, 1s, 30s) " +
            "SHUFFLE BY ipv4_dst_ip " +
            "GROUP BY window_start, ipv4_dst_ip " +
            "HAVING sum_bytes > %s " +
            "SETTINGS max_threads=%s", ddos_mv, victims_stream, ddos_rule_threshold, ddos_mv_threads);

    static class Victim {
        public final long victim_ip;
        public final long sum_bytes;
        public final Timestamp timestamp;

        public Victim(long victim_ip, long sum_bytes, Timestamp timestamp) {
            this.victim_ip = victim_ip;
            this.sum_bytes = sum_bytes;
            this.timestamp = timestamp;
        }
    }

    private final BlockingQueue<Victim> victims = new LinkedBlockingQueue<>();

    private final ExecutorService executor = Executors.newFixedThreadPool(8);

    void ingestIPFixFlow() {
        executor.execute(() -> {
            try (Connection connection = DriverManager.getConnection(timeplus_jdbc_url)) {
                try (Statement stmt = connection.createStatement()) {
                    /// The query is a long running insert since the select is a streaming query
                    /// This emulates continuous data ingestion
                    stmt.executeQuery("INSERT INTO ipfix SELECT * FROM ipfix_random");
                }
            } catch (SQLException sql_ex) {
                System.out.println("Failed to ingest ipfix flow data, " + sql_ex.getStackTrace().toString());
            }
        });
    }

    void doReportAttackers(Connection connection, String victim_ips, Timestamp attack_start_time) throws Exception {
        String top_k = "1000";
        String report_attackers_sql = String.format(
                "with contributors as (" +
                        " select ipv4_dst_ip, ipv4_src_ip, sum(byte_count) as src_ip_sum_bytes, min(_tp_time) as src_start_time" +
                        "    from table(%s)" +
                        "    where _tp_time >= '%s' and ipv4_dst_ip in (%s)" +
                        "    group by ipv4_dst_ip, ipv4_src_ip " +
                        "), " +
                        "ranked AS (" +
                        "    select ipv4_dst_ip, ipv4_src_ip, src_ip_sum_bytes, src_start_time" +
                        ", row_number() over (partition by ipv4_dst_ip ORDER BY src_ip_sum_bytes DESC) as rank from contributors " +
                        ") " +
                        "SELECT * FROM ranked where rank <= %s;", ipfix_stream, attack_start_time.toString(), victim_ips, top_k);

        /// System.out.printf("Running table query to find out attackers and their contributions, query=%s\n", report_attackers_sql);
        System.out.printf("--------------------------Finding attackers at %s\n", attack_start_time);

        try (PreparedStatement pstmt = connection.prepareStatement(report_attackers_sql)) {
            try (ResultSet attacker_rs = pstmt.executeQuery()) {
                while (attacker_rs.next()) {
                    System.out.printf(
                            "victim_ip=%d attacker_ip=%d attacker_bytes=%d src_start_time=%s\n",
                            attacker_rs.getLong(1), attacker_rs.getLong(2), attacker_rs.getLong(3), attacker_rs.getTimestamp(4));
                }
            }
        }
    }

    void reportAttackers(Connection connection) throws Exception {
        /// Build the victim IPs
        Timestamp attack_start_time = null;
        StringBuilder victim_ips = new StringBuilder();

        /// Always running
        while (true) {
            Victim victim = victims.poll(50, TimeUnit.MILLISECONDS);
            if (victim == null) {
                String this_round_of_victims = victim_ips.toString();
                if (!this_round_of_victims.isEmpty() && attack_start_time != null) {
                    doReportAttackers(connection, this_round_of_victims, attack_start_time);
                    victim_ips.setLength(0);
                    attack_start_time = null;
                }

                continue;
            }

            if (attack_start_time == null) {
                attack_start_time = victim.timestamp;
            } else {
                if (attack_start_time.compareTo(victim.timestamp) == 0) {
                    victim_ips.append(",");
                } else {
                    /// Run into another emit, finish last round first
                    /// This usually means we are slow at reporting the attackers
                    doReportAttackers(connection, victim_ips.toString(), attack_start_time);

                    victim_ips.setLength(0);
                    attack_start_time = victim.timestamp;
                }
            }

            victim_ips.append(victim.victim_ip);
        }
    }

    void consumeVictims() throws Exception {
        String victims_query = String.format("select victim_ip, sum_bytes, _tp_time from %s", victims_stream);

        executor.execute(() -> {
            try (Connection connection = DriverManager.getConnection(timeplus_jdbc_url)) {
                try (PreparedStatement pstmt = connection.prepareStatement(victims_query)) {
                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            /// victim_ip, sum_bytes, _tp_time
                            victims.add(new Victim(rs.getLong(1), rs.getLong(2), rs.getTimestamp(3)));
                        }
                    }
                }
            } catch (SQLException sql_ex) {
                System.out.println("Failed to ingest ipfix flow data, " + sql_ex.getStackTrace().toString());
            }
        });
    }

    void prepare(Connection connection) throws Exception {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeQuery("drop stream if exists ddos_mv");
            stmt.executeQuery("drop stream if exists ipfix_random");
            stmt.executeQuery("drop stream if exists ipfix");
            stmt.executeQuery("drop stream if exists victims");
            stmt.executeQuery("drop stream if exists attackers");

            stmt.executeQuery(ipfix_random_ddl);
            stmt.executeQuery(ipfix_ddl);
            stmt.executeQuery(victims_ddl);
            stmt.executeQuery(rule_threshold_detection_mv);
        }
    }

    void run() throws Exception {
        Connection adhoc_connection = DriverManager.getConnection(timeplus_jdbc_url);

        /// Emulation
        /// ipfix_random (ingest data) -> ipfix <- ddos_mv (do streaming aggregation) -> victims <- attackers_view (find out top / all attackers)

        /// Create streams / materialized views
        prepare(adhoc_connection);

        /// Emulate ipfix data generation
        ingestIPFixFlow();

        /// After the data is generated, the Materialized View \ddos_mv will evaluate the
        /// victim destination IPs incrementally and continuously and if there are rule threshold
        /// violations, emit the victim IPs to \victims stream
        /// We just need monitor if there are new events in \victims stream via a streaming query
        /// and message the victims to attackers reporter
        consumeVictims();

        /// Report the attackers for the victims
        reportAttackers(adhoc_connection);
    }

    public static void main(String[] args) throws Exception {
        DDosDetectionAndReport ddos = new DDosDetectionAndReport();
        ddos.run();
    }
}
