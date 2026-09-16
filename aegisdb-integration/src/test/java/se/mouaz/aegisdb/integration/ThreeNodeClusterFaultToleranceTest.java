package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Validates real fault scenarios in a 3-node Raft consensus cluster:
 * 1. Leader hard crash -> automated re-election -> failover writes -> recovery.
 * 2. Follower hard crash -> quorum availability -> reconnect and catch-up.
 * 3. Asymmetric network partition (majority 2 vs minority 1) -> minority rejection -> partition healing.
 * 4. Total quorum loss (2 of 3 offline) -> safety preservation -> quorum restoration.
 */
@DisplayName("3-Node Cluster Fault Tolerance & Partition Test Suite")
class ThreeNodeClusterFaultToleranceTest {

    private NodeId id1;
    private NodeId id2;
    private NodeId id3;

    private InMemoryTransport transport1;
    private InMemoryTransport transport2;
    private InMemoryTransport transport3;

    private RaftNode node1;
    private RaftNode node2;
    private RaftNode node3;

    private ClusterConfiguration clusterConfig;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        id1 = NodeId.of("cluster-node-1");
        id2 = NodeId.of("cluster-node-2");
        id3 = NodeId.of("cluster-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 7201);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 7202);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 7203);

        clusterConfig = ClusterConfiguration.builder()
                .clusterId("fault-cluster")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        transport1 = new InMemoryTransport(id1);
        transport2 = new InMemoryTransport(id2);
        transport3 = new InMemoryTransport(id3);

        transport1.start();
        transport2.start();
        transport3.start();

        // Node 1 is seeded with a faster timeout to reliably become initial leader
        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(80))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(201))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(600))
                .maxElectionTimeout(Duration.ofMillis(900))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(202))
                .build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(600))
                .maxElectionTimeout(Duration.ofMillis(900))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(203))
                .build();

        node1.start();
        node2.start();
        node3.start();
    }

    @AfterEach
    void tearDown() {
        gracefulStop(node1);
        gracefulStop(node2);
        gracefulStop(node3);

        if (transport1 != null) transport1.stop();
        if (transport2 != null) transport2.stop();
        if (transport3 != null) transport3.stop();

        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    private void gracefulStop(RaftNode node) {
        if (node != null) {
            try {
                node.stop();
            } catch (Exception ignored) {
            }
        }
    }

    private void hardCrash(RaftNode node, InMemoryTransport transport) {
        // Abrupt termination: sever transport instantly without graceful state machine stop/flush
        if (transport != null) {
            transport.stop();
        }
        if (node != null) {
            try {
                node.stop();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    @DisplayName("Scenario 1: Leader hard crash triggers re-election, failover writes, and rejoins cleanly")
    void leaderHardCrashAndFailoverRecovery() throws Exception {
        // 1. Wait for initial leader
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);

        // 2. Commit 2 initial entries
        node1.propose("entry-1".getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
        node1.propose("entry-2".getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
        assertThat(node1.commitIndex()).isEqualTo(2L);
        await().atMost(Duration.ofSeconds(2)).until(() -> node2.commitIndex() == 2L && node3.commitIndex() == 2L);

        // 3. Simulate hard crash on Leader (Node 1)
        hardCrash(node1, transport1);

        // 4. Remaining nodes (quorum 2/3) must detect timeout and elect a new leader
        await().atMost(Duration.ofSeconds(4)).until(() ->
                node2.role() == RaftRole.LEADER || node3.role() == RaftRole.LEADER);

        RaftNode newLeader = (node2.role() == RaftRole.LEADER) ? node2 : node3;
        assertThat(newLeader.currentTerm()).isGreaterThan(1L);

        // 5. Failover writes succeed on the new leader
        CompletableFuture<Long> f3 = newLeader.propose("entry-3".getBytes(StandardCharsets.UTF_8));
        assertThat(f3.get(3, TimeUnit.SECONDS)).isEqualTo(3L);
        CompletableFuture<Long> f4 = newLeader.propose("entry-4".getBytes(StandardCharsets.UTF_8));
        assertThat(f4.get(3, TimeUnit.SECONDS)).isEqualTo(4L);

        // 6. Restart old leader (Node 1)
        transport1 = new InMemoryTransport(id1);
        transport1.start();
        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(300))
                .maxElectionTimeout(Duration.ofMillis(450))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(204))
                .build();
        node1.start();

        // 7. Node 1 rejoins as follower and catches up to index 4
        await().atMost(Duration.ofSeconds(4)).until(() -> node1.commitIndex() == 4L);
        assertThat(node1.role()).isEqualTo(RaftRole.FOLLOWER);
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(newLeader.log(), node1.log(), 4L);
    }

    @Test
    @DisplayName("Scenario 2: Follower hard crash does not block quorum, follower catches up upon restart")
    void followerHardCrashAndCatchup() throws Exception {
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);

        // 1. Abruptly kill follower Node 3
        hardCrash(node3, transport3);

        // 2. Writes continue to commit via surviving quorum (Node 1 + Node 2)
        for (int i = 1; i <= 3; i++) {
            CompletableFuture<Long> f = node1.propose(("quorum-write-" + i).getBytes(StandardCharsets.UTF_8));
            assertThat(f.get(3, TimeUnit.SECONDS)).isEqualTo((long) i);
        }
        assertThat(node1.commitIndex()).isEqualTo(3L);
        await().atMost(Duration.ofSeconds(2)).until(() -> node2.commitIndex() == 3L);

        // 3. Restart Node 3
        transport3 = new InMemoryTransport(id3);
        transport3.start();
        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(300))
                .maxElectionTimeout(Duration.ofMillis(450))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(205))
                .build();
        node3.start();

        // 4. Node 3 synchronizes missing entries and reaches commitIndex 3
        await().atMost(Duration.ofSeconds(4)).until(() -> node3.commitIndex() == 3L);
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), 3L);
    }

    @Test
    @DisplayName("Scenario 3: Asymmetric network partition (majority 2 vs minority 1)")
    void networkPartitionTwoVsOne() throws Exception {
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);

        // Commit initial entry
        node1.propose("init-val".getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);

        // 1. Partition cluster: Group A = {Node 1, Node 2} (majority), Group B = {Node 3} (minority)
        InMemoryTransport.partition(Set.of(id1, id2), Set.of(id3));

        // 2. Majority partition {Node 1, Node 2} can still commit writes
        CompletableFuture<Long> majorityFuture = node1.propose("majority-write".getBytes(StandardCharsets.UTF_8));
        Long majorityIdx = majorityFuture.get(3, TimeUnit.SECONDS);
        assertThat(majorityIdx).isEqualTo(2L);
        assertThat(node1.commitIndex()).isEqualTo(2L);

        // 3. Minority node (Node 3) cannot commit writes (rejects as follower or non-quorate node)
        CompletableFuture<Long> minorityFuture = node3.propose("minority-write".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> minorityFuture.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(se.mouaz.aegisdb.raft.NotLeaderException.class);
        assertThat(node3.commitIndex()).isEqualTo(1L);

        // 4. Heal network partition
        InMemoryTransport.healPartitions();

        // 5. Node 3 reconciles and updates its commit index to match majority
        await().atMost(Duration.ofSeconds(4)).until(() -> node3.commitIndex() == 2L);
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), 2L);
    }

    @Test
    @DisplayName("Scenario 4: Total quorum loss halts commits, resumes safely upon reconnection")
    void quorumLossAndRecovery() throws Exception {
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);

        // Disconnect both followers -> Node 1 is alone (1/3 < majority)
        transport2.stop();
        transport3.stop();

        // Writes must stall and not commit
        CompletableFuture<Long> stalledFuture = node1.propose("stalled-cmd".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> stalledFuture.get(200, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
        assertThat(node1.commitIndex()).isZero();

        // Reconnect followers -> Quorum restored
        transport2.start();
        transport3.start();
        node1.replicationManager().replicateTo(id2);
        node1.replicationManager().replicateTo(id3);

        // Stalled command completes commit
        Long committedIdx = stalledFuture.get(3, TimeUnit.SECONDS);
        assertThat(committedIdx).isEqualTo(1L);
        assertThat(node1.commitIndex()).isEqualTo(1L);

        await().atMost(Duration.ofSeconds(3)).until(() -> node2.commitIndex() == 1L && node3.commitIndex() == 1L);
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node2.log(), 1L);
    }
}
