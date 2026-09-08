package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.snapshot.SnapshotManager;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import se.mouaz.aegisdb.raft.statemachine.KvCommand;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies Sprint 5 Acceptance Criteria:
 * US009: As a database node, I want snapshots so the log does not grow without bound.
 * [AC2] Snapshot install via chunked InstallSnapshot RPC.
 * [AC5] Follower catch-up from snapshot: Slow/disconnected follower whose log entries
 *       were compacted catches up via InstallSnapshot.
 */
class InstallSnapshotCatchupIntegrationTest {

    private NodeId id1;
    private NodeId id2;
    private NodeId id3;

    private ClusterConfiguration clusterConfig;

    private InMemoryTransport transport1;
    private InMemoryTransport transport2;
    private InMemoryTransport transport3;

    private KeyValueStateMachine sm1;
    private KeyValueStateMachine sm2;
    private KeyValueStateMachine sm3;

    private RaftLog log1;
    private RaftLog log2;
    private RaftLog log3;

    private RaftNode node1;
    private RaftNode node2;
    private RaftNode node3;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        id1 = NodeId.of("snap-node-1");
        id2 = NodeId.of("snap-node-2");
        id3 = NodeId.of("snap-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 12001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 12002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 12003);

        clusterConfig = ClusterConfiguration.builder()
                .clusterId("snap-cluster")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        transport1 = new InMemoryTransport(id1);
        transport2 = new InMemoryTransport(id2);
        transport3 = new InMemoryTransport(id3);

        sm1 = new KeyValueStateMachine();
        sm2 = new KeyValueStateMachine();
        sm3 = new KeyValueStateMachine();

        log1 = new RaftLog();
        log2 = new RaftLog();
        log3 = new RaftLog();
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
    }

    @Test
    @DisplayName("[US009 / AC5] Lagging follower whose log entries are compacted catches up via InstallSnapshot")
    void testFollowerCatchupViaInstallSnapshot() throws Exception {
        // --- 1. Start Node 1 (fast election timeout) and Node 2 ---
        // Node 3 is NOT started yet (simulating offline/lagging follower)
        transport1.start();
        transport2.start();

        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .persistentState(new PersistentRaftState())
                .raftLog(log1)
                .stateMachine(sm1)
                .minElectionTimeout(Duration.ofMillis(80))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(25))
                .random(new Random(1))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .persistentState(new PersistentRaftState())
                .raftLog(log2)
                .stateMachine(sm2)
                .minElectionTimeout(Duration.ofMillis(800))
                .maxElectionTimeout(Duration.ofMillis(1000))
                .heartbeatInterval(Duration.ofMillis(25))
                .random(new Random(2))
                .build();

        node1.start();
        node2.start();

        // Wait for Node 1 to become leader of term 1
        await().atMost(5, TimeUnit.SECONDS).until(() -> node1.role() == RaftRole.LEADER);
        assertThat(node1.currentTerm()).isGreaterThanOrEqualTo(1);

        // --- 2. Commit 20 KV entries to majority (node 1 + node 2) while node 3 is offline ---
        for (int i = 1; i <= 20; i++) {
            byte[] cmd = KvCommand.put("key-" + i, ("val-" + i).getBytes(StandardCharsets.UTF_8)).toBytes();
            CompletableFuture<byte[]> writeFuture = node1.executeClientCommand(cmd);
            writeFuture.get(5, TimeUnit.SECONDS);
        }

        // Verify state on active nodes
        await().atMost(5, TimeUnit.SECONDS).until(() -> sm1.get("key-20") != null);
        await().atMost(5, TimeUnit.SECONDS).until(() -> sm2.get("key-20") != null);
        assertThat(new String(sm1.get("key-20"), StandardCharsets.UTF_8)).isEqualTo("val-20");
        assertThat(new String(sm2.get("key-20"), StandardCharsets.UTF_8)).isEqualTo("val-20");

        // Leader commit index should be at least 20
        long leaderCommit = node1.commitIndex();
        assertThat(leaderCommit).isGreaterThanOrEqualTo(20);

        // --- 3. Leader takes a snapshot and compacts log up to entry 20 ---
        node1.takeSnapshot(20, node1.currentTerm());
        assertThat(log1.snapshotIndex()).isEqualTo(20);

        // Verify that entry 10 is no longer available in log entries list (it is compacted)
        assertThat(log1.getEntry(10)).isEmpty();

        // --- 4. Node 3 starts up with empty log and state machine ---
        transport3.start();
        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .persistentState(new PersistentRaftState())
                .raftLog(log3)
                .stateMachine(sm3)
                .minElectionTimeout(Duration.ofMillis(800))
                .maxElectionTimeout(Duration.ofMillis(1000))
                .heartbeatInterval(Duration.ofMillis(25))
                .random(new Random(3))
                .build();

        node3.start();

        // Node 1's ReplicationManager will see node 3's nextIndex = 1 <= snapshotIndex 20,
        // and trigger chunked InstallSnapshot!
        // Await node 3 state machine catching up with all 20 keys via snapshot installation
        await().atMost(10, TimeUnit.SECONDS).until(() -> sm3.get("key-20") != null);

        for (int i = 1; i <= 20; i++) {
            byte[] val = sm3.get("key-" + i);
            assertThat(val).as("Key " + i + " must exist on node 3 after snapshot catch-up").isNotNull();
            assertThat(new String(val, StandardCharsets.UTF_8)).isEqualTo("val-" + i);
        }

        // --- 5. Verify normal log replication continues after snapshot catchup ---
        for (int i = 21; i <= 25; i++) {
            byte[] cmd = KvCommand.put("key-" + i, ("val-" + i).getBytes(StandardCharsets.UTF_8)).toBytes();
            CompletableFuture<byte[]> writeFuture = node1.executeClientCommand(cmd);
            writeFuture.get(5, TimeUnit.SECONDS);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> sm3.get("key-25") != null);
        assertThat(new String(sm3.get("key-25"), StandardCharsets.UTF_8)).isEqualTo("val-25");
    }
}
