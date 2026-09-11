package se.mouaz.aegisdb.observability;

import se.mouaz.aegisdb.common.NodeId;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * AegisMetrics provides thread-safe, high-performance operational metrics collection
 * and Prometheus exposition format serialization for AegisDB nodes.
 *
 * Conforms to Master Project Plan §15 (Observability and Operational Diagnostics):
 * Metrics:
 * - request_count, read_count, write_count
 * - transaction_count, transaction_abort_count
 * - leader_election_count
 * - raft_log_size, replication_lag, wal_size
 * - snapshot_count, snapshot_duration, recovery_duration
 * - p50_latency, p95_latency, p99_latency, throughput
 */
public final class AegisMetrics {

    private static final AegisMetrics GLOBAL_INSTANCE = new AegisMetrics("global");

    private final String nodeId;
    private final long startTimeMs = System.currentTimeMillis();

    // Counters
    private final LongAdder requestCount = new LongAdder();
    private final LongAdder readCount = new LongAdder();
    private final LongAdder writeCount = new LongAdder();
    private final LongAdder transactionCount = new LongAdder();
    private final LongAdder transactionAbortCount = new LongAdder();
    private final LongAdder leaderElectionCount = new LongAdder();
    private final LongAdder snapshotCount = new LongAdder();

    // Gauges
    private final AtomicLong raftLogSize = new AtomicLong(0);
    private final AtomicLong replicationLag = new AtomicLong(0);
    private final AtomicLong walSize = new AtomicLong(0);
    private final AtomicLong currentTerm = new AtomicLong(0);

    // Latency tracking (sliding window for percentile calculation)
    private static final int MAX_LATENCY_SAMPLES = 5000;
    private final ConcurrentLinkedDeque<Long> requestLatenciesNanos = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<Long> replicationLatenciesNanos = new ConcurrentLinkedDeque<>();

    // Additional custom metrics tags
    private final Map<String, LongAdder> customCounters = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> customGauges = new ConcurrentHashMap<>();

    public AegisMetrics(String nodeId) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId must not be null");
    }

    public static AegisMetrics global() {
        return GLOBAL_INSTANCE;
    }

    public static AegisMetrics forNode(NodeId nodeId) {
        return new AegisMetrics(nodeId.value());
    }

    public String getNodeId() {
        return nodeId;
    }

    // --- Counter Recording ---

    public void recordRequest() {
        requestCount.increment();
    }

    public void recordRead() {
        readCount.increment();
        recordRequest();
    }

    public void recordWrite() {
        writeCount.increment();
        recordRequest();
    }

    public void recordTransaction(boolean committed) {
        transactionCount.increment();
        if (!committed) {
            transactionAbortCount.increment();
        }
    }

    public void recordLeaderElection() {
        leaderElectionCount.increment();
    }

    public void recordSnapshot() {
        snapshotCount.increment();
    }

    // --- Gauge Recording ---

    public void setRaftLogSize(long size) {
        raftLogSize.set(size);
    }

    public void setReplicationLag(long lag) {
        replicationLag.set(lag);
    }

    public void setWalSize(long bytes) {
        walSize.set(bytes);
    }

    public void setCurrentTerm(long term) {
        currentTerm.set(term);
    }

    // --- Latency Recording ---

    public void recordRequestLatencyNanos(long latencyNanos) {
        requestLatenciesNanos.addLast(latencyNanos);
        if (requestLatenciesNanos.size() > MAX_LATENCY_SAMPLES) {
            requestLatenciesNanos.pollFirst();
        }
    }

    public void recordReplicationLatencyNanos(long latencyNanos) {
        replicationLatenciesNanos.addLast(latencyNanos);
        if (replicationLatenciesNanos.size() > MAX_LATENCY_SAMPLES) {
            replicationLatenciesNanos.pollFirst();
        }
    }

    // --- Percentile & Throughput Calculation ---

    public double getThroughputOpsPerSec() {
        long uptimeSec = Math.max(1, (System.currentTimeMillis() - startTimeMs) / 1000);
        return (double) requestCount.sum() / uptimeSec;
    }

    public double getPercentileLatencyMs(double percentile) {
        List<Long> samples = new ArrayList<>(requestLatenciesNanos);
        if (samples.isEmpty()) {
            return 0.0;
        }
        Collections.sort(samples);
        int index = (int) Math.ceil((percentile / 100.0) * samples.size()) - 1;
        index = Math.clamp(index, 0, samples.size() - 1);
        return samples.get(index) / 1_000_000.0;
    }

    public double getP50LatencyMs() {
        return getPercentileLatencyMs(50.0);
    }

    public double getP95LatencyMs() {
        return getPercentileLatencyMs(95.0);
    }

    public double getP99LatencyMs() {
        return getPercentileLatencyMs(99.0);
    }

    // --- Getters ---

    public long getRequestCount() { return requestCount.sum(); }
    public long getReadCount() { return readCount.sum(); }
    public long getWriteCount() { return writeCount.sum(); }
    public long getTransactionCount() { return transactionCount.sum(); }
    public long getTransactionAbortCount() { return transactionAbortCount.sum(); }
    public long getLeaderElectionCount() { return leaderElectionCount.sum(); }
    public long getSnapshotCount() { return snapshotCount.sum(); }
    public long getRaftLogSize() { return raftLogSize.get(); }
    public long getReplicationLag() { return replicationLag.get(); }
    public long getWalSize() { return walSize.get(); }
    public long getCurrentTerm() { return currentTerm.get(); }

    public double getTransactionAbortRate() {
        long total = transactionCount.sum();
        return total == 0 ? 0.0 : (double) transactionAbortCount.sum() / total;
    }

    // --- Prometheus Text Format Export ---

    /**
     * Formats all collected metrics into standard Prometheus OpenMetrics text format.
     */
    public String exportPrometheusText() {
        StringBuilder sb = new StringBuilder(2048);
        String nodeLabel = "{node=\"" + nodeId + "\"}";

        appendMetric(sb, "aegisdb_uptime_seconds", "Total uptime of the AegisDB node in seconds", "gauge",
                (System.currentTimeMillis() - startTimeMs) / 1000.0, nodeLabel);

        appendMetric(sb, "aegisdb_request_total", "Total requests received", "counter",
                requestCount.sum(), nodeLabel);
        appendMetric(sb, "aegisdb_read_total", "Total read operations", "counter",
                readCount.sum(), nodeLabel);
        appendMetric(sb, "aegisdb_write_total", "Total write operations", "counter",
                writeCount.sum(), nodeLabel);
        appendMetric(sb, "aegisdb_transaction_total", "Total transactions executed", "counter",
                transactionCount.sum(), nodeLabel);
        appendMetric(sb, "aegisdb_transaction_abort_total", "Total aborted transactions", "counter",
                transactionAbortCount.sum(), nodeLabel);
        appendMetric(sb, "aegisdb_leader_election_total", "Total leader elections observed", "counter",
                leaderElectionCount.sum(), nodeLabel);
        appendMetric(sb, "aegisdb_snapshot_total", "Total snapshots created", "counter",
                snapshotCount.sum(), nodeLabel);

        appendMetric(sb, "aegisdb_raft_log_size", "Current Raft log entry count", "gauge",
                raftLogSize.get(), nodeLabel);
        appendMetric(sb, "aegisdb_replication_lag", "Current replication lag behind leader in entries", "gauge",
                replicationLag.get(), nodeLabel);
        appendMetric(sb, "aegisdb_wal_size_bytes", "Current WAL storage size on disk in bytes", "gauge",
                walSize.get(), nodeLabel);
        appendMetric(sb, "aegisdb_raft_term", "Current Raft consensus term", "gauge",
                currentTerm.get(), nodeLabel);

        appendMetric(sb, "aegisdb_latency_p50_milliseconds", "P50 request latency in milliseconds", "gauge",
                getP50LatencyMs(), nodeLabel);
        appendMetric(sb, "aegisdb_latency_p95_milliseconds", "P95 request latency in milliseconds", "gauge",
                getP95LatencyMs(), nodeLabel);
        appendMetric(sb, "aegisdb_latency_p99_milliseconds", "P99 tail latency in milliseconds", "gauge",
                getP99LatencyMs(), nodeLabel);
        appendMetric(sb, "aegisdb_throughput_ops_per_second", "Current operational throughput in ops/sec", "gauge",
                getThroughputOpsPerSec(), nodeLabel);
        appendMetric(sb, "aegisdb_transaction_abort_rate", "Fraction of transactions that aborted", "gauge",
                getTransactionAbortRate(), nodeLabel);

        return sb.toString();
    }

    private void appendMetric(StringBuilder sb, String name, String help, String type, Number value, String labels) {
        sb.append("# HELP ").append(name).append(" ").append(help).append("\n");
        sb.append("# TYPE ").append(name).append(" ").append(type).append("\n");
        sb.append(name).append(labels).append(" ").append(value).append("\n");
    }

    public void reset() {
        requestCount.reset();
        readCount.reset();
        writeCount.reset();
        transactionCount.reset();
        transactionAbortCount.reset();
        leaderElectionCount.reset();
        snapshotCount.reset();
        raftLogSize.set(0);
        replicationLag.set(0);
        walSize.set(0);
        currentTerm.set(0);
        requestLatenciesNanos.clear();
        replicationLatenciesNanos.clear();
    }
}
