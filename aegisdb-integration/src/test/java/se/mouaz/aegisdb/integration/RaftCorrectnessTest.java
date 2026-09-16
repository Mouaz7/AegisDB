package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end executable test suite verifying core Raft safety invariants:
 * 1. Election Safety: at most one leader can be elected per term.
 * 2. Absence of leader heartbeats causes follower election timeout and new election.
 * 3. Log entries replicate to a majority before commit index advances.
 * 4. Committed entries are never overwritten (Leader Append-Only & Log Matching).
 * 5. Conflicting uncommitted follower logs are truncated and repaired by the leader.
 * 6. Restarted follower catches up with missed committed entries.
 */
@DisplayName("Raft Correctness Invariants Test Suite")
class RaftCorrectnessTest {

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

        id1 = NodeId.of("node-1");
        id2 = NodeId.of("node-2");
        id3 = NodeId.of("node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 7101);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 7102);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 7103);

        clusterConfig = ClusterConfiguration.builder()
                .clusterId("correctness-cluster")
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

        // Node 1 is seeded with a lower election timeout to bias initial leadership
        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(80))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(101))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(600))
                .maxElectionTimeout(Duration.ofMillis(900))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(102))
                .build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(600))
                .maxElectionTimeout(Duration.ofMillis(900))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(103))
                .build();

        node1.start();
        node2.start();
        node3.start();
    }

    @AfterEach
    void tearDown() {
        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
        if (node3 != null) node3.stop();

        if (transport1 != null) transport1.stop();
        if (transport2 != null) transport2.stop();
        if (transport3 != null) transport3.stop();

        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("1. Election Safety: At most one leader can be elected per term")
    void electionSafetyAtMostOneLeaderPerTerm() {
        // Wait for initial election
        await().atMost(Duration.ofSeconds(3)).until(() ->
                List.of(node1, node2, node3).stream().anyMatch(n -> n.role() == RaftRole.LEADER));

        Map<Long, NodeId> leadersPerTerm = new ConcurrentHashMap<>();

        // Record leader for term 1
        for (RaftNode node : List.of(node1, node2, node3)) {
            if (node.role() == RaftRole.LEADER) {
                NodeId prev = leadersPerTerm.put(node.currentTerm(), node.nodeId());
                assertThat(prev).as("Term %d must have at most 1 leader", node.currentTerm()).isNull();
            }
        }

        // Induce second election: stop current leader
        RaftNode currentLeader = List.of(node1, node2, node3).stream()
                .filter(n -> n.role() == RaftRole.LEADER).findFirst().orElseThrow();
        currentLeader.stop();

        // Remaining 2 nodes must elect exactly one new leader in a higher term
        await().atMost(Duration.ofSeconds(4)).until(() -> {
            List<RaftNode> remaining = List.of(node1, node2, node3).stream()
                    .filter(n -> n != currentLeader).toList();
            return remaining.stream().anyMatch(n -> n.role() == RaftRole.LEADER);
        });

        for (RaftNode node : List.of(node1, node2, node3)) {
            if (node != currentLeader && node.role() == RaftRole.LEADER) {
                NodeId prev = leadersPerTerm.put(node.currentTerm(), node.nodeId());
                assertThat(prev).as("Term %d must have at most 1 leader", node.currentTerm()).isNull();
                assertThat(node.currentTerm()).isGreaterThan(1L);
            }
        }
    }

    @Test
    @DisplayName("2. Absence of leader heartbeats causes follower election timeout and new election")
    void absenceOfLeaderHeartbeatsCausesFollowerElectionTimeoutAndNewElection() {
        // 1. Await initial election with Node 1 as leader
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);
        long initialTerm = node1.currentTerm();

        // 2. Sever leader transport so heartbeats cease completely
        transport1.stop();

        // 3. Followers must detect missed heartbeats via election timeout and trigger new election
        await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
            boolean newLeaderElected = (node2.role() == RaftRole.LEADER || node3.role() == RaftRole.LEADER);
            assertThat(newLeaderElected).isTrue();
            long newTerm = Math.max(node2.currentTerm(), node3.currentTerm());
            assertThat(newTerm).isGreaterThan(initialTerm);
        });
    }

    @Test
    @DisplayName("3. Entries replicate to majority before commit index advances")
    void entriesReplicateToMajorityBeforeCommit() throws Exception {
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);

        // Disconnect Node 2 & Node 3 -> Node 1 cannot achieve quorum
        transport2.stop();
        transport3.stop();

        CompletableFuture<Long> unquorateFuture = node1.propose("unquorate-entry".getBytes(StandardCharsets.UTF_8));

        // Commit index must NOT advance without majority acknowledgment
        assertThatThrownBy(() -> unquorateFuture.get(200, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
        assertThat(node1.commitIndex()).isEqualTo(0L);

        // Restore Node 2 -> Quorum (2/3: Node 1 + Node 2) is restored
        transport2.start();
        node1.replicationManager().replicateTo(id2);

        // Entry must now be acknowledged and commit index advanced
        Long committedIndex = unquorateFuture.get(3, TimeUnit.SECONDS);
        assertThat(committedIndex).isEqualTo(1L);
        assertThat(node1.commitIndex()).isEqualTo(1L);
        await().atMost(Duration.ofSeconds(2)).until(() -> node2.commitIndex() == 1L);
    }

    @Test
    @DisplayName("4. Committed entries are never overwritten across leader changes")
    void committedEntriesNeverOverwritten() throws Exception {
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);

        // Propose and commit 3 entries across the full cluster
        for (int i = 1; i <= 3; i++) {
            CompletableFuture<Long> fut = node1.propose(("committed-data-" + i).getBytes(StandardCharsets.UTF_8));
            Long idx = fut.get(3, TimeUnit.SECONDS);
            assertThat(idx).isEqualTo((long) i);
        }

        assertThat(node1.commitIndex()).isEqualTo(3L);
        await().atMost(Duration.ofSeconds(2)).until(() -> node2.commitIndex() == 3L && node3.commitIndex() == 3L);

        // Crash current leader (Node 1)
        node1.stop();
        transport1.stop();

        // Node 2 or 3 is elected in a new term
        await().atMost(Duration.ofSeconds(4)).until(() ->
                node2.role() == RaftRole.LEADER || node3.role() == RaftRole.LEADER);

        RaftNode newLeader = (node2.role() == RaftRole.LEADER) ? node2 : node3;

        // Verify that entries 1..3 remain committed and unmodified in the new leader's log
        assertThat(newLeader.commitIndex()).isGreaterThanOrEqualTo(3L);
        for (int i = 1; i <= 3; i++) {
            assertThat(newLeader.log().getEntry(i)).isPresent();
            assertThat(new String(newLeader.log().getEntry(i).get().data(), StandardCharsets.UTF_8))
                    .isEqualTo("committed-data-" + i);
        }

        // Invariant: Logs of Node 2 and Node 3 must match identically on committed entries
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node2.log(), node3.log(), 3L);
    }

    @Test
    @DisplayName("5. Conflicting uncommitted follower logs are truncated and repaired by the leader")
    void conflictingLogsTruncatedAndRepaired() throws Exception {
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);

        // Commit entry 1 to all nodes
        CompletableFuture<Long> fut = node1.propose("initial-entry".getBytes(StandardCharsets.UTF_8));
        fut.get(5, TimeUnit.SECONDS);
        await().atMost(Duration.ofSeconds(6)).until(() -> node3.commitIndex() == 1L);

        // Disconnect Node 3
        transport3.stop();

        // Append divergent uncommitted entry directly into Node 3's uncommitted log space (different uncommitted term)
        node3.log().append(new RaftLogEntry(2L, 99L, "conflicting-stale-data".getBytes(StandardCharsets.UTF_8)));
        assertThat(node3.log().lastLogIndex()).isEqualTo(2L);
        assertThat(new String(node3.log().getEntry(2L).get().data(), StandardCharsets.UTF_8))
                .isEqualTo("conflicting-stale-data");

        // Leader commits 2 new entries (indices 2 and 3) with Node 2
        for (int i = 2; i <= 3; i++) {
            CompletableFuture<Long> f = node1.propose(("leader-entry-" + i).getBytes(StandardCharsets.UTF_8));
            f.get(5, TimeUnit.SECONDS);
        }
        assertThat(node1.commitIndex()).isEqualTo(3L);

        // Reconnect Node 3
        transport3.start();
        node1.replicationManager().replicateTo(id3);

        // Node 3 must truncate its conflicting index 2 and adopt the leader's entry 2 and 3
        await().atMost(Duration.ofSeconds(8)).until(() -> node3.commitIndex() == 3L);
        assertThat(new String(node3.log().getEntry(2L).get().data(), StandardCharsets.UTF_8))
                .isEqualTo("leader-entry-2");
        assertThat(new String(node3.log().getEntry(3L).get().data(), StandardCharsets.UTF_8))
                .isEqualTo("leader-entry-3");

        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), 3L);
    }

    @Test
    @DisplayName("6. Restarted follower catches up with missed committed entries")
    void restartedFollowerCatchesUp() throws Exception {
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);

        // Disconnect Node 3
        transport3.stop();

        // Node 1 and Node 2 commit 5 entries
        for (int i = 1; i <= 5; i++) {
            CompletableFuture<Long> f = node1.propose(("catchup-cmd-" + i).getBytes(StandardCharsets.UTF_8));
            f.get(3, TimeUnit.SECONDS);
        }
        assertThat(node1.commitIndex()).isEqualTo(5L);

        // Node 3 lagged at index 0
        assertThat(node3.log().lastLogIndex()).isEqualTo(0L);

        // Reconnect Node 3
        transport3.start();
        node1.replicationManager().replicateTo(id3);

        // Node 3 catches up to index 5
        await().atMost(Duration.ofSeconds(8)).until(() -> node3.commitIndex() == 5L);
        assertThat(node3.log().lastLogIndex()).isEqualTo(5L);

        for (int i = 1; i <= 5; i++) {
            assertThat(node3.log().getEntry(i)).isPresent();
            assertThat(new String(node3.log().getEntry(i).get().data(), StandardCharsets.UTF_8))
                    .isEqualTo("catchup-cmd-" + i);
        }
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), 5L);
    }
}
