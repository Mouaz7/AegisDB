package se.mouaz.aegisdb.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark addressing Research Question 1 (Master Project Plan §20):
 * "How does Raft write batching affect throughput and tail latency in a Java-based replicated database?"
 *
 * Dimension: Raft batch size (1, 10, 50, 100).
 * Measures: Throughput (ops/sec), P50 latency (ms), P95 latency (ms), P99 latency (ms).
 */
public class RQ1BatchingBenchmark {
    private static final Logger log = LoggerFactory.getLogger(RQ1BatchingBenchmark.class);

    public static List<BenchmarkResult> runSuite(int totalOps, long seed) throws Exception {
        int[] batchSizes = {1, 10, 50, 100};
        List<BenchmarkResult> results = new ArrayList<>();

        for (int batchSize : batchSizes) {
            results.add(runSingle(batchSize, totalOps, seed));
        }

        return results;
    }

    public static BenchmarkResult runSingle(int batchSize, int totalOps, long seed) throws Exception {
        log.info("Starting RQ1 Benchmark with batchSize={}, totalOps={}", batchSize, totalOps);

        NodeId n1 = NodeId.of("rq1-node-1");
        NodeId n2 = NodeId.of("rq1-node-2");
        NodeId n3 = NodeId.of("rq1-node-3");

        InMemoryTransport t1 = new InMemoryTransport(n1);
        InMemoryTransport t2 = new InMemoryTransport(n2);
        InMemoryTransport t3 = new InMemoryTransport(n3);

        t1.start();
        t2.start();
        t3.start();

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId(new se.mouaz.aegisdb.common.ClusterId("rq1-cluster"))
                .addMember(n1, new Endpoint("localhost", 8001))
                .addMember(n2, new Endpoint("localhost", 8002))
                .addMember(n3, new Endpoint("localhost", 8003))
                .build();

        RaftNode node1 = RaftNode.builder()
                .nodeId(n1).clusterConfig(clusterConfig).transport(t1)
                .minElectionTimeout(Duration.ofMillis(80))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(1))
                .build();

        RaftNode node2 = RaftNode.builder()
                .nodeId(n2).clusterConfig(clusterConfig).transport(t2)
                .minElectionTimeout(Duration.ofMillis(800))
                .maxElectionTimeout(Duration.ofMillis(1000))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(2))
                .build();

        RaftNode node3 = RaftNode.builder()
                .nodeId(n3).clusterConfig(clusterConfig).transport(t3)
                .minElectionTimeout(Duration.ofMillis(800))
                .maxElectionTimeout(Duration.ofMillis(1000))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(3))
                .build();

        try {
            node1.start();
            node2.start();
            node3.start();

            BenchmarkUtils.awaitCondition(() -> node1.role() == RaftRole.LEADER, 15000);

            List<Long> latenciesNanos = new ArrayList<>(totalOps);
            long successfulOps = 0;
            long failedOps = 0;

            long startNano = System.nanoTime();

            int remaining = totalOps;
            int opIndex = 0;

            while (remaining > 0) {
                int currentBatch = Math.min(batchSize, remaining);
                long batchStart = System.nanoTime();

                List<CompletableFuture<Long>> futures = new ArrayList<>(currentBatch);
                for (int b = 0; b < currentBatch; b++) {
                    byte[] payload = ("key-" + opIndex + "=val-" + opIndex).getBytes(StandardCharsets.UTF_8);
                    futures.add(node1.propose(payload));
                    opIndex++;
                }

                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                        .get(15, TimeUnit.SECONDS);

                long batchDuration = System.nanoTime() - batchStart;
                long perOpLatency = batchDuration / currentBatch;
                for (int b = 0; b < currentBatch; b++) {
                    latenciesNanos.add(perOpLatency);
                }

                successfulOps += currentBatch;
                remaining -= currentBatch;
            }

            long totalDurationNanos = System.nanoTime() - startNano;
            double elapsedSec = totalDurationNanos / 1_000_000_000.0;
            double throughput = (successfulOps / elapsedSec);

            double p50 = BenchmarkUtils.calculatePercentile(latenciesNanos, 50.0);
            double p95 = BenchmarkUtils.calculatePercentile(latenciesNanos, 95.0);
            double p99 = BenchmarkUtils.calculatePercentile(latenciesNanos, 99.0);
            double max = BenchmarkUtils.calculatePercentile(latenciesNanos, 100.0);

            BenchmarkConfig config = BenchmarkConfig.builder("RQ1_RaftBatching")
                    .clusterSize(3)
                    .clientCount(1)
                    .batchSize(batchSize)
                    .totalOperations(totalOps)
                    .seed(seed)
                    .workload("WRITE_REPLICATION")
                    .failureScenario("NONE")
                    .build();

            return new BenchmarkResult(
                    config,
                    totalOps,
                    successfulOps,
                    failedOps,
                    elapsedSec,
                    throughput,
                    p50,
                    p95,
                    p99,
                    max,
                    0.0,
                    0.0,
                    BenchmarkUtils.captureMetadata(seed)
            );
        } finally {
            node1.stop();
            node2.stop();
            node3.stop();
            t1.stop();
            t2.stop();
            t3.stop();
        }
    }
}
