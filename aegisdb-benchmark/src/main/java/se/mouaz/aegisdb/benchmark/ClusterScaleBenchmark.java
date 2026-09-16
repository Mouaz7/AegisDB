package se.mouaz.aegisdb.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.client.DefaultAegisDbClient;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark runner evaluating throughput, tail latencies (p50, p90, p95, p99),
 * election time, recovery time, and resource profiling across 3, 5, and 7 nodes.
 */
public class ClusterScaleBenchmark {

    private static final Logger log = LoggerFactory.getLogger(ClusterScaleBenchmark.class);

    public enum Workload {
        READ_HEAVY("Read-Heavy (90% Read, 10% Write)"),
        WRITE_HEAVY("Write-Heavy (90% Write, 10% Read)"),
        CONTENDED("Contended (50% Update on 5 Hot Keys)");

        private final String description;
        Workload(String desc) { this.description = desc; }
        public String description() { return description; }
    }

    public record BenchmarkRow(
            int nodeCount,
            Workload workload,
            double throughputOpsSec,
            double p50Ms,
            double p90Ms,
            double p95Ms,
            double p99Ms,
            double maxMs,
            double abortRate,
            long electionTimeMs,
            long recoveryTimeMs,
            ProfilingMetricsCollector.ProfilingReport profiling
    ) {}

    public static void main(String[] args) throws Exception {
        System.out.println("Starting AegisDB Multi-Node Scale Benchmark (3, 5, 7 nodes)...");
        List<BenchmarkRow> results = runAll(2000L); // 2 seconds per scenario for quick validation

        File outputDir = new File("experiments/results");
        outputDir.mkdirs();

        // Write Markdown table
        File mdFile = new File(outputDir, "scale_benchmarks.md");
        try (PrintWriter pw = new PrintWriter(new FileWriter(mdFile))) {
            pw.println("# AegisDB Multi-Node Scale & Profiling Evaluation");
            pw.println();
            pw.println("| Nodes | Workload | Throughput (ops/s) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) | Abort Rate | Election (ms) | Recovery (ms) |");
            pw.println("| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |");
            for (BenchmarkRow r : results) {
                pw.printf("| %d | %s | %.1f | %.2f | %.2f | %.2f | %.2f | %.2f | %.2f%% | %d | %d |\n",
                        r.nodeCount(), r.workload().name(), r.throughputOpsSec(),
                        r.p50Ms(), r.p90Ms(), r.p95Ms(), r.p99Ms(), r.maxMs(),
                        r.abortRate(), r.electionTimeMs(), r.recoveryTimeMs());
            }
            pw.println();
            pw.println("## Resource Profiling Summary");
            pw.println();
            for (BenchmarkRow r : results) {
                pw.printf("### %d Nodes - %s\n", r.nodeCount(), r.workload().name());
                pw.println(r.profiling().toMarkdown());
                pw.println();
            }
        }

        // Write CSV
        File csvFile = new File(outputDir, "scale_benchmarks.csv");
        try (PrintWriter pw = new PrintWriter(new FileWriter(csvFile))) {
            pw.println("nodes,workload,throughput_ops_sec,p50_ms,p90_ms,p95_ms,p99_ms,max_ms,abort_rate_pct,election_ms,recovery_ms,cpu_pct,gc_pause_ms");
            for (BenchmarkRow r : results) {
                pw.printf("%d,%s,%.1f,%.3f,%.3f,%.3f,%.3f,%.3f,%.2f,%d,%d,%.1f,%d\n",
                        r.nodeCount(), r.workload().name(), r.throughputOpsSec(),
                        r.p50Ms(), r.p90Ms(), r.p95Ms(), r.p99Ms(), r.maxMs(),
                        r.abortRate(), r.electionTimeMs(), r.recoveryTimeMs(),
                        r.profiling().avgProcessCpuPercent(), r.profiling().totalGcPauseTimeMs());
            }
        }

        System.out.println("Benchmarks successfully exported to " + mdFile.getAbsolutePath() + " and " + csvFile.getAbsolutePath());
    }

    public static List<BenchmarkRow> runAll(long testDurationMs) throws Exception {
        List<BenchmarkRow> rows = new ArrayList<>();
        int[] clusterSizes = {3, 5, 7};
        Workload[] workloads = {Workload.READ_HEAVY, Workload.WRITE_HEAVY, Workload.CONTENDED};

        for (int size : clusterSizes) {
            for (Workload wl : workloads) {
                log.info("Running benchmark: {} nodes, workload={}", size, wl);
                BenchmarkRow row = runSingleScenario(size, wl, testDurationMs);
                rows.add(row);
            }
        }
        return rows;
    }

    public static BenchmarkRow runSingleScenario(int nodeCount, Workload workload, long durationMs) throws Exception {
        List<NodeId> nodeIds = new ArrayList<>();
        ClusterConfiguration.Builder cb = ClusterConfiguration.builder().clusterId("bench-cluster-" + nodeCount);
        for (int i = 1; i <= nodeCount; i++) {
            NodeId id = NodeId.of("node-" + nodeCount + "-" + i);
            nodeIds.add(id);
            cb.addMember(id, Endpoint.of("127.0.0.1", 20000 + (nodeCount * 10) + i));
        }
        ClusterConfiguration clusterConfig = cb.build();

        List<InMemoryTransport> transports = new ArrayList<>();
        List<RaftNode> nodes = new ArrayList<>();
        Map<NodeId, RaftNode> activeMap = new ConcurrentHashMap<>();

        long electionStart = System.currentTimeMillis();
        for (int i = 0; i < nodeCount; i++) {
            NodeId id = nodeIds.get(i);
            InMemoryTransport transport = new InMemoryTransport(id);
            transports.add(transport);
            transport.start();

            int minElection = 80 + (i * 60);
            int maxElection = minElection + 40;

            RaftNode node = RaftNode.builder()
                    .nodeId(id)
                    .clusterConfig(clusterConfig)
                    .transport(transport)
                    .persistentState(new PersistentRaftState())
                    .stateMachine(new KeyValueStateMachine())
                    .minElectionTimeout(Duration.ofMillis(minElection))
                    .maxElectionTimeout(Duration.ofMillis(maxElection))
                    .heartbeatInterval(Duration.ofMillis(20))
                    .random(new Random(700 + i))
                    .build();

            nodes.add(node);
            activeMap.put(id, node);
            node.start();
        }

        // Wait for leader election
        RaftNode leaderNode = nodes.get(0);
        long electionDeadline = System.currentTimeMillis() + 5000L;
        while (leaderNode.role() != RaftRole.LEADER && System.currentTimeMillis() < electionDeadline) {
            Thread.sleep(20);
        }
        long electionTimeMs = System.currentTimeMillis() - electionStart;

        DefaultAegisDbClient client = DefaultAegisDbClient.forNodes(activeMap);

        // Warm up with 5 writes
        for (int k = 1; k <= 5; k++) {
            client.putString("warmup:" + k, "val").get(3, TimeUnit.SECONDS);
        }

        // Benchmark run with profiling
        ProfilingMetricsCollector.Snapshot snapStart = ProfilingMetricsCollector.takeSnapshot();
        long benchStartNs = System.nanoTime();

        List<Long> latencies = new CopyOnWriteArrayList<>();
        AtomicLong totalOps = new AtomicLong(0);
        AtomicLong abortOps = new AtomicLong(0);
        AtomicBoolean running = new AtomicBoolean(true);

        int clientThreads = 4;
        ExecutorService pool = Executors.newFixedThreadPool(clientThreads);
        List<Future<?>> tasks = new ArrayList<>();

        for (int t = 0; t < clientThreads; t++) {
            final int threadId = t;
            tasks.add(pool.submit(() -> {
                int opIndex = 0;
                while (running.get()) {
                    opIndex++;
                    String key = (workload == Workload.CONTENDED)
                            ? "hot-key-" + (opIndex % 5)
                            : "key-" + threadId + "-" + (opIndex % 100);

                    boolean isRead;
                    if (workload == Workload.READ_HEAVY) {
                        isRead = (opIndex % 10 != 0); // 90% read
                    } else if (workload == Workload.WRITE_HEAVY) {
                        isRead = (opIndex % 10 == 0); // 10% read
                    } else {
                        isRead = (opIndex % 2 == 0); // 50% update
                    }

                    long start = System.nanoTime();
                    try {
                        if (isRead) {
                            client.getString(key).get(800, TimeUnit.MILLISECONDS);
                        } else {
                            client.putString(key, "data-" + opIndex).get(800, TimeUnit.MILLISECONDS);
                        }
                        long duration = System.nanoTime() - start;
                        latencies.add(duration);
                        totalOps.incrementAndGet();
                    } catch (Exception e) {
                        abortOps.incrementAndGet();
                    }
                }
            }));
        }

        Thread.sleep(durationMs);
        running.set(false);

        for (Future<?> f : tasks) {
            f.get(4, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        long benchDurationNs = System.nanoTime() - benchStartNs;
        ProfilingMetricsCollector.Snapshot snapEnd = ProfilingMetricsCollector.takeSnapshot();
        ProfilingMetricsCollector.ProfilingReport profiling = ProfilingMetricsCollector.diff(snapStart, snapEnd);

        double durationSec = benchDurationNs / 1_000_000_000.0;
        double throughput = totalOps.get() / durationSec;
        double abortRate = (abortOps.get() * 100.0) / Math.max(1, totalOps.get() + abortOps.get());

        double p50 = BenchmarkUtils.calculatePercentile(latencies, 50.0);
        double p90 = BenchmarkUtils.calculatePercentile(latencies, 90.0);
        double p95 = BenchmarkUtils.calculatePercentile(latencies, 95.0);
        double p99 = BenchmarkUtils.calculatePercentile(latencies, 99.0);
        double max = BenchmarkUtils.calculatePercentile(latencies, 100.0);

        // Measure simulated recovery latency
        long recStart = System.currentTimeMillis();
        leaderNode.stop();
        // Wait for next leader
        long recDeadline = System.currentTimeMillis() + 4000L;
        while (System.currentTimeMillis() < recDeadline) {
            boolean hasNewLeader = nodes.stream().skip(1).anyMatch(n -> n.role() == RaftRole.LEADER);
            if (hasNewLeader) break;
            Thread.sleep(20);
        }
        long recoveryTimeMs = System.currentTimeMillis() - recStart;

        // Cleanup
        client.close();
        for (RaftNode n : nodes) {
            try { n.stop(); } catch (Exception ignored) {}
        }
        for (InMemoryTransport t : transports) {
            try { t.stop(); } catch (Exception ignored) {}
        }
        InMemoryTransport.clearRegistry();

        return new BenchmarkRow(
                nodeCount,
                workload,
                throughput,
                p50,
                p90,
                p95,
                p99,
                max,
                abortRate,
                electionTimeMs,
                recoveryTimeMs,
                profiling
        );
    }
}
