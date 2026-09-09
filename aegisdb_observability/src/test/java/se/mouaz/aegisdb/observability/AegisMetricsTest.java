package se.mouaz.aegisdb.observability;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AegisMetricsTest {

    private AegisMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new AegisMetrics("test-node-1");
    }

    @Test
    void shouldTrackCountersAccurately() {
        metrics.recordRead();
        metrics.recordWrite();
        metrics.recordWrite();
        metrics.recordLeaderElection();
        metrics.recordSnapshot();

        assertThat(metrics.getRequestCount()).isEqualTo(3);
        assertThat(metrics.getReadCount()).isEqualTo(1);
        assertThat(metrics.getWriteCount()).isEqualTo(2);
        assertThat(metrics.getLeaderElectionCount()).isEqualTo(1);
        assertThat(metrics.getSnapshotCount()).isEqualTo(1);
    }

    @Test
    void shouldTrackTransactionsAndAbortRate() {
        metrics.recordTransaction(true);
        metrics.recordTransaction(true);
        metrics.recordTransaction(false);

        assertThat(metrics.getTransactionCount()).isEqualTo(3);
        assertThat(metrics.getTransactionAbortCount()).isEqualTo(1);
        assertThat(metrics.getTransactionAbortRate()).isCloseTo(0.3333, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void shouldCalculateLatencyPercentilesCorrectly() {
        for (long i = 1; i <= 100; i++) {
            metrics.recordRequestLatencyNanos(i * 1_000_000L); // 1ms to 100ms
        }

        assertThat(metrics.getP50LatencyMs()).isCloseTo(50.0, org.assertj.core.data.Offset.offset(2.0));
        assertThat(metrics.getP95LatencyMs()).isCloseTo(95.0, org.assertj.core.data.Offset.offset(2.0));
        assertThat(metrics.getP99LatencyMs()).isCloseTo(99.0, org.assertj.core.data.Offset.offset(2.0));
    }

    @Test
    void shouldFormatPrometheusTextExposition() {
        metrics.recordRead();
        metrics.recordWrite();
        metrics.setRaftLogSize(42);
        metrics.setCurrentTerm(5);

        String prom = metrics.exportPrometheusText();

        assertThat(prom).contains("aegisdb_request_total{node=\"test-node-1\"} 2");
        assertThat(prom).contains("aegisdb_read_total{node=\"test-node-1\"} 1");
        assertThat(prom).contains("aegisdb_write_total{node=\"test-node-1\"} 1");
        assertThat(prom).contains("aegisdb_raft_log_size{node=\"test-node-1\"} 42");
        assertThat(prom).contains("aegisdb_raft_term{node=\"test-node-1\"} 5");
        assertThat(prom).contains("# TYPE aegisdb_request_total counter");
        assertThat(prom).contains("# TYPE aegisdb_raft_log_size gauge");
    }
}
