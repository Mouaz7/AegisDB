package se.mouaz.aegisdb.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.chaos.FaultRule;
import se.mouaz.aegisdb.chaos.FaultType;
import se.mouaz.aegisdb.chaos.FaultyTransport;
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
 * Benchmark addressing Research Question 2 (Master Project Plan §20):
 * "How do leader failures and network delays affect availability, recovery time, and P99 latency?"
 *
 * Dimensions:
 * - Failure scenarios: BASELINE (None), NETWORK_DELAY, LEADER_KILL
 * Measures: Availability (success rate), Recovery duration (ms), P99 latency (ms), Throughput.
 */
public class RQ2FailureRecoveryBenchmark {
    private static final Logger log = LoggerFactory.getLogger(RQ2FailureRecoveryBenchmark.class);

    public static List<BenchmarkResult> runSuite(int opsPerScenario, long seed) throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        results.add(runBaseline(opsPerScenario, seed));
        results.add(runNetworkDelay(opsPerScenario, seed, 15)); // 15ms delay
        results.add(runLeaderFailover(opsPerScenario, seed));
        return results;
    }

    public static BenchmarkResult runBaseline(int totalOps, long seed) throws Exception {
        return executeScenario("RQ2_Baseline", "NONE", 0, false, totalOps, seed);
    }

    public static BenchmarkResult runNetworkDelay(int totalOps, long seed, int delayMs) throws Exception {
        return executeScenario("RQ2_NetworkDelay", "DELAY_" + delayMs + "MS", delayMs, false, totalOps, seed);
    }

    public static BenchmarkResult runLeaderFailover(int totalOps, long seed) throws Exception {
        return executeScenario("RQ2_LeaderFailover", "LEADER_KILL", 0, true, totalOps, seed);
    }

    private static BenchmarkResult executeScenario(String expName, String failureScenario,
                                                   int delayMs, boolean killLeader,
                                                   int totalOps, long seed) throws Exception {
        log.info("Executing RQ2 Scenario: {}, delayMs={}, killLeader={}, ops={}", expName, delayMs, killLeader, totalOps);

        NodeId n1 = NodeId.of("rq2-node-1");
        NodeId n2 = NodeId.of("rq2-node-2");
        NodeId n3 = NodeId.of("rq2-node-3");

        InMemoryTransport base1 = new InMemoryTransport(n1);
        InMemoryTransport base2 = new InMemoryTransport(n2);
        InMemoryTransport base3 = new InMemoryTransport(n3);

        FaultyTransport t1 = new FaultyTransport(base1, seed);
        FaultyTransport t2 = new FaultyTransport(base2, seed);
        FaultyTransport t3 = new FaultyTransport(base3, seed);

        if (delayMs > 0) {
            t1.addRule(FaultRule.delay("rule-delay-1", null, null, Duration.ofMillis(delayMs), 1.0));
            t2.addRule(FaultRule.delay("rule-delay-2", null, null, Duration.ofMillis(delayMs), 1.0));
            t3.addRule(FaultRule.delay("rule-delay-3", null, null, Duration.ofMillis(delayMs), 1.0));
        }

        t1.start();
        t2.start();
        t3.start();

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId(new se.mouaz.aegisdb.common.ClusterId("rq2-cluster"))
                .addMember(n1, new Endpoint("localhost", 8101))
                .addMember(n2, new Endpoint("localhost", 8102))
                .addMember(n3, new Endpoint("localhost", 8103))
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
                .minElectionTimeout(Duration.ofMillis(250))
                .maxElectionTimeout(Duration.ofMillis(350))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(2))
                .build();

        RaftNode node3 = RaftNode.builder()
                .nodeId(n3).clusterConfig(clusterConfig).transport(t3)
                .minElectionTimeout(Duration.ofMillis(400))
                .maxElectionTimeout(Duration.ofMillis(500))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(3))
                .build();

        node1.start();
        node2.start();
        node3.start();

        List<Long> latenciesNanos = new ArrayList<>();
        long successOps = 0;
        long failedOps = 0;
        double recoveryTimeMs = 0.0;

        try {
            BenchmarkUtils.awaitCondition(() -> node1.role() == RaftRole.LEADER, 5000);

            long startTime = System.nanoTime();
            int midPoint = totalOps / 2;

            RaftNode currentLeader = node1;

            for (int i = 0; i < totalOps; i++) {
                if (killLeader && i == midPoint) {
                    log.info("Triggering leader kill at op {}/{}", i, totalOps);
                    long killTime = System.nanoTime();
                    currentLeader.stop();

                    // Wait for node2 or node3 to win new election automatically
                    BenchmarkUtils.awaitCondition(() ->
                            node2.role() == RaftRole.LEADER || node3.role() == RaftRole.LEADER, 5000);

                    long recoveryTime = System.nanoTime();
                    recoveryTimeMs = (recoveryTime - killTime) / 1_000_000.0;
                    currentLeader = (node2.role() == RaftRole.LEADER) ? node2 : node3;
                    log.info("Failover complete in {} ms! New leader is {}", recoveryTimeMs, currentLeader.nodeId());
                }

                long opStart = System.nanoTime();
                try {
                    byte[] payload = ("rq2-key-" + i + "=value").getBytes(StandardCharsets.UTF_8);
                    CompletableFuture<Long> fut = currentLeader.propose(payload);
                    fut.get(1500, TimeUnit.MILLISECONDS);
                    latenciesNanos.add(System.nanoTime() - opStart);
                    successOps++;
                } catch (Exception ex) {
                    failedOps++;
                }
            }

            long totalDuration = System.nanoTime() - startTime;
            double elapsedSec = totalDuration / 1_000_000_000.0;
            double throughput = (successOps / elapsedSec);

            double p50 = BenchmarkUtils.calculatePercentile(latenciesNanos, 50.0);
            double p95 = BenchmarkUtils.calculatePercentile(latenciesNanos, 95.0);
            double p99 = BenchmarkUtils.calculatePercentile(latenciesNanos, 99.0);
            double max = BenchmarkUtils.calculatePercentile(latenciesNanos, 100.0);
            double errorRate = (totalOps > 0) ? ((double) failedOps / totalOps) : 0.0;

            BenchmarkConfig config = BenchmarkConfig.builder(expName)
                    .clusterSize(3)
                    .clientCount(1)
                    .batchSize(1)
                    .totalOperations(totalOps)
                    .seed(seed)
                    .workload("WRITE_REPLICATION")
                    .failureScenario(failureScenario)
                    .build();

            return new BenchmarkResult(
                    config,
                    totalOps,
                    successOps,
                    failedOps,
                    elapsedSec,
                    throughput,
                    p50,
                    p95,
                    p99,
                    max,
                    errorRate,
                    recoveryTimeMs,
                    BenchmarkUtils.captureMetadata(seed)
            );
        } finally {
            try { node1.stop(); } catch (Exception ignored) {}
            try { node2.stop(); } catch (Exception ignored) {}
            try { node3.stop(); } catch (Exception ignored) {}
            t1.stop();
            t2.stop();
            t3.stop();
        }
    }
}
