package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.chaos.FaultRule;
import se.mouaz.aegisdb.chaos.FaultType;
import se.mouaz.aegisdb.chaos.FaultyTransport;
import se.mouaz.aegisdb.chaos.linearizability.LinearizabilityResult;
import se.mouaz.aegisdb.chaos.linearizability.OperationStatus;
import se.mouaz.aegisdb.chaos.linearizability.OperationTrace;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@DisplayName("Combined Multi-Vector Chaos Cluster Testing with Seed Replay")
class CombinedChaosClusterTest {

    private static final Logger log = LoggerFactory.getLogger(CombinedChaosClusterTest.class);

    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<InMemoryTransport> baseTransports = new ArrayList<>();
    private final List<FaultyTransport> faultyTransports = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (RaftNode n : nodes) {
            if (n != null) {
                try { n.stop(); } catch (Exception ignored) {}
            }
        }
        for (InMemoryTransport t : baseTransports) {
            if (t != null) {
                try { t.stop(); } catch (Exception ignored) {}
            }
        }
        for (FaultyTransport ft : faultyTransports) {
            if (ft != null) {
                try { ft.stop(); } catch (Exception ignored) {}
            }
        }
        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("3-Node Cluster: Combined leader crash, 2v1 partition, and stochastic packet drop")
    void threeNodeCombinedChaosScenario() throws Exception {
        long seed = 42001L;
        log.info("Starting 3-Node Combined Chaos Test with SEED={}", seed);
        Random rng = new Random(seed);

        NodeId id1 = NodeId.of("chaos-3-1");
        NodeId id2 = NodeId.of("chaos-3-2");
        NodeId id3 = NodeId.of("chaos-3-3");

        ClusterConfiguration config = ClusterConfiguration.builder()
                .clusterId("chaos-3-cluster")
                .addMember(id1, Endpoint.of("127.0.0.1", 16001))
                .addMember(id2, Endpoint.of("127.0.0.1", 16002))
                .addMember(id3, Endpoint.of("127.0.0.1", 16003))
                .build();

        InMemoryTransport t1 = new InMemoryTransport(id1);
        InMemoryTransport t2 = new InMemoryTransport(id2);
        InMemoryTransport t3 = new InMemoryTransport(id3);
        baseTransports.addAll(List.of(t1, t2, t3));

        FaultyTransport ft1 = new FaultyTransport(t1, rng.nextLong());
        FaultyTransport ft2 = new FaultyTransport(t2, rng.nextLong());
        FaultyTransport ft3 = new FaultyTransport(t3, rng.nextLong());
        faultyTransports.addAll(List.of(ft1, ft2, ft3));

        // Inject 10% stochastic packet drop on AppendEntries across cluster
        FaultRule dropRule = FaultRule.builder("drop-10", FaultType.DROP)
                .probability(0.10)
                .forRpc(AppendEntriesRequest.class)
                .build();
        ft1.addRule(dropRule);
        ft2.addRule(dropRule);
        ft3.addRule(dropRule);

        t1.start(); t2.start(); t3.start();
        ft1.start(); ft2.start(); ft3.start();

        RaftNode n1 = RaftNode.builder().nodeId(id1).clusterConfig(config).transport(ft1)
                .persistentState(new PersistentRaftState())
                .minElectionTimeout(Duration.ofMillis(150)).maxElectionTimeout(Duration.ofMillis(220))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(rng.nextLong())).build();

        RaftNode n2 = RaftNode.builder().nodeId(id2).clusterConfig(config).transport(ft2)
                .persistentState(new PersistentRaftState())
                .minElectionTimeout(Duration.ofMillis(250)).maxElectionTimeout(Duration.ofMillis(400))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(rng.nextLong())).build();

        RaftNode n3 = RaftNode.builder().nodeId(id3).clusterConfig(config).transport(ft3)
                .persistentState(new PersistentRaftState())
                .minElectionTimeout(Duration.ofMillis(250)).maxElectionTimeout(Duration.ofMillis(400))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(rng.nextLong())).build();

        nodes.addAll(List.of(n1, n2, n3));
        n1.start(); n2.start(); n3.start();

        try {
            // 1. Initial election under 10% packet drop
            await().atMost(8, TimeUnit.SECONDS).until(() -> nodes.stream().anyMatch(n -> n.role() == RaftRole.LEADER));
            RaftNode initialLeader = nodes.stream().filter(n -> n.role() == RaftRole.LEADER).findFirst().orElseThrow();
            CompletableFuture<Long> f1 = initialLeader.propose("init-1".getBytes(StandardCharsets.UTF_8));
            assertThat(f1.get(5, TimeUnit.SECONDS)).isEqualTo(1L);

            // Ensure majority quorum commit sync
            await().atMost(5, TimeUnit.SECONDS).until(() ->
                    nodes.stream().filter(n -> n != initialLeader).anyMatch(n -> n.commitIndex() >= 1L));

            // 2. Kill initial leader abruptly
            initialLeader.stop();
            int leaderIdx = nodes.indexOf(initialLeader);
            baseTransports.get(leaderIdx).stop();
            faultyTransports.get(leaderIdx).stop();

            // 3. Surviving majority elects new leader despite packet drop
            List<RaftNode> survivors = nodes.stream().filter(n -> n != initialLeader).toList();
            RaftNode newLeader = null;
            for (int attempt = 0; attempt < 5; attempt++) {
                await().atMost(8, TimeUnit.SECONDS).until(() -> survivors.stream().anyMatch(n -> n.role() == RaftRole.LEADER));
                RaftNode currentLeader = survivors.stream().filter(n -> n.role() == RaftRole.LEADER).findFirst().orElseThrow();
                try {
                    CompletableFuture<Long> f2 = currentLeader.propose("survivor-write".getBytes(StandardCharsets.UTF_8));
                    assertThat(f2.get(4, TimeUnit.SECONDS)).isGreaterThanOrEqualTo(2L);
                    newLeader = currentLeader;
                    break;
                } catch (Exception ignored) {
                    // leader changed during packet drop, retry with newly elected leader
                }
            }
            assertThat(newLeader).isNotNull();
            final RaftNode electedLeader = newLeader;

            // 4. Asymmetric 2v1 partition: isolate the other survivor node
            RaftNode survivorPeer = survivors.stream().filter(n -> n != electedLeader).findFirst().orElseThrow();
            InMemoryTransport.partition(Set.of(electedLeader.nodeId()), Set.of(survivorPeer.nodeId()));

            // Alone leader cannot reach majority
            CompletableFuture<Long> stalledWrite = electedLeader.propose("stalled-write".getBytes(StandardCharsets.UTF_8));
            assertThatThrownBy(() -> stalledWrite.get(300, TimeUnit.MILLISECONDS))
                    .satisfies(t -> assertThat(t).matches(ex ->
                            ex instanceof TimeoutException ||
                            (ex instanceof ExecutionException ee && ee.getCause() instanceof se.mouaz.aegisdb.raft.NotLeaderException)));

            // Heal partition and remove packet drop rule to allow clean recovery
            InMemoryTransport.healPartitions();
            ft2.removeRule("drop-10");
            ft3.removeRule("drop-10");

            Long finalIdx = null;
            for (int attempt = 0; attempt < 5; attempt++) {
                try {
                    await().atMost(6, TimeUnit.SECONDS).until(() -> survivors.stream().anyMatch(n -> n.role() == RaftRole.LEADER));
                    RaftNode healedLeader = survivors.stream().filter(n -> n.role() == RaftRole.LEADER).findFirst().orElseThrow();
                    CompletableFuture<Long> fHealed = healedLeader.propose("healed-write".getBytes(StandardCharsets.UTF_8));
                    finalIdx = fHealed.get(4, TimeUnit.SECONDS);
                    break;
                } catch (Exception ignored) {
                    // wait briefly before retrying if leader election in progress
                    Thread.sleep(100);
                }
            }
            assertThat(finalIdx).isNotNull().isGreaterThanOrEqualTo(2L);

            RaftInvariants.assertIdenticalOrderOfCommittedEntries(survivors.get(0).log(), survivors.get(1).log(), 2L);

        } catch (Throwable t) {
            log.error("Test failed with SEED={}, fault status: ft1_dropped={}, ft2_dropped={}, ft3_dropped={}",
                    seed, ft1.droppedMessagesCount(), ft2.droppedMessagesCount(), ft3.droppedMessagesCount());
            throw t;
        }
    }

    @Test
    @DisplayName("5-Node Cluster: 3v2 asymmetric partition, packet delays, and linearizability verification")
    void fiveNodePartitionAndLinearizabilityScenario() throws Exception {
        long seed = 55002L;
        log.info("Starting 5-Node Combined Chaos Test with SEED={}", seed);
        Random rng = new Random(seed);

        List<NodeId> nodeIds = new ArrayList<>();
        ClusterConfiguration.Builder configBuilder = ClusterConfiguration.builder().clusterId("chaos-5-cluster");

        for (int i = 1; i <= 5; i++) {
            NodeId id = NodeId.of("chaos-5-" + i);
            nodeIds.add(id);
            configBuilder.addMember(id, Endpoint.of("127.0.0.1", 17000 + i));
        }
        ClusterConfiguration clusterConfig = configBuilder.build();

        OperationTrace trace = new OperationTrace();

        for (int i = 0; i < 5; i++) {
            NodeId id = nodeIds.get(i);
            InMemoryTransport baseTransport = new InMemoryTransport(id);
            baseTransports.add(baseTransport);

            FaultyTransport ft = new FaultyTransport(baseTransport, rng.nextLong());
            faultyTransports.add(ft);

            baseTransport.start();
            ft.start();

            // Stagger election timeouts so Node 1 is preferred initial leader
            int minTimeout = 100 + (i * 80);
            int maxTimeout = minTimeout + 60;

            RaftNode node = RaftNode.builder()
                    .nodeId(id)
                    .clusterConfig(clusterConfig)
                    .transport(ft)
                    .persistentState(new PersistentRaftState())
                    .minElectionTimeout(Duration.ofMillis(minTimeout))
                    .maxElectionTimeout(Duration.ofMillis(maxTimeout))
                    .heartbeatInterval(Duration.ofMillis(25))
                    .random(new Random(rng.nextLong()))
                    .build();

            nodes.add(node);
            node.start();
        }

        RaftNode node1 = nodes.get(0);
        await().atMost(6, TimeUnit.SECONDS).until(() -> node1.role() == RaftRole.LEADER);

        try {
            // Write initial entry
            long inv1 = System.nanoTime();
            node1.propose("init-val".getBytes(StandardCharsets.UTF_8)).get(4, TimeUnit.SECONDS);
            long ret1 = System.nanoTime();
            trace.recordWrite("client", "key", "init-val", inv1, ret1, OperationStatus.OK);

            // Wait for majority commit replication
            await().atMost(4, TimeUnit.SECONDS).until(() ->
                    nodes.get(1).commitIndex() >= 1L && nodes.get(2).commitIndex() >= 1L
            );

            // 1. Asymmetric 3v2 Partition:
            // Majority Group A = {node1, node2, node3}
            // Minority Group B = {node4, node5}
            Set<NodeId> groupA = Set.of(nodeIds.get(0), nodeIds.get(1), nodeIds.get(2));
            Set<NodeId> groupB = Set.of(nodeIds.get(3), nodeIds.get(4));
            InMemoryTransport.partition(groupA, groupB);

            // 2. Inject 5ms delay on majority group to test overlapping concurrency
            FaultRule delayRule = FaultRule.builder("delay-5ms", FaultType.DELAY)
                    .delay(Duration.ofMillis(5))
                    .probability(0.5)
                    .build();
            faultyTransports.get(0).addRule(delayRule);
            faultyTransports.get(1).addRule(delayRule);
            faultyTransports.get(2).addRule(delayRule);

            // 3. Writes succeed on majority partition (3/5 is quorum)
            long inv2 = System.nanoTime();
            CompletableFuture<Long> majorityFuture = node1.propose("majority-val".getBytes(StandardCharsets.UTF_8));
            Long committedIdx = majorityFuture.get(5, TimeUnit.SECONDS);
            long ret2 = System.nanoTime();
            assertThat(committedIdx).isEqualTo(2L);
            trace.recordWrite("client", "key", "majority-val", inv2, ret2, OperationStatus.OK);

            // 4. Writes to minority partition node fail (cannot reach quorum or not leader)
            CompletableFuture<Long> minorityFuture = nodes.get(3).propose("minority-val".getBytes(StandardCharsets.UTF_8));
            assertThatThrownBy(() -> minorityFuture.get(500, TimeUnit.MILLISECONDS))
                    .hasCauseInstanceOf(se.mouaz.aegisdb.raft.NotLeaderException.class);

            // 5. Heal partition
            InMemoryTransport.healPartitions();
            faultyTransports.get(0).removeRule("delay-5ms");
            faultyTransports.get(1).removeRule("delay-5ms");
            faultyTransports.get(2).removeRule("delay-5ms");

            // 6. Minority nodes synchronize log and reach commitIndex 2
            await().atMost(6, TimeUnit.SECONDS).until(() ->
                    nodes.get(3).commitIndex() == 2L && nodes.get(4).commitIndex() == 2L
            );

            // 7. Verify all 5 nodes have bit-identical logs
            for (int i = 1; i < 5; i++) {
                RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), nodes.get(i).log(), 2L);
            }

            // 8. Verify linearizability of the entire execution
            LinearizabilityResult linResult = trace.verifyLinearizability();
            assertThat(linResult.isLinearizable()).isTrue();

        } catch (Throwable t) {
            log.error("5-Node test failed with SEED={}. Reproduction parameters saved.", seed);
            throw t;
        }
    }
}
