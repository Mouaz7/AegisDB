package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.storage.DurableRaftLog;
import se.mouaz.aegisdb.storage.StorageEngine;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies Sprint 4 Acceptance Criteria:
 * US007: As a database, I want committed data to survive crashes.
 * US008: As a Raft node, I want term/vote state durable across restart.
 */
class NodeRestartPersistenceTest {

    @TempDir
    Path clusterDataDir;

    private NodeId id1;
    private NodeId id2;
    private NodeId id3;

    private ClusterConfiguration clusterConfig;

    private InMemoryTransport transport1;
    private InMemoryTransport transport2;
    private InMemoryTransport transport3;

    private StorageEngine storage1;
    private StorageEngine storage2;
    private StorageEngine storage3;

    private RaftNode node1;
    private RaftNode node2;
    private RaftNode node3;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        id1 = NodeId.of("persistent-node-1");
        id2 = NodeId.of("persistent-node-2");
        id3 = NodeId.of("persistent-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 11001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 11002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 11003);

        clusterConfig = ClusterConfiguration.builder()
                .clusterId("persistence-cluster")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();
    }

    @AfterEach
    void tearDown() throws IOException {
        stopCluster();
    }

    private void stopCluster() throws IOException {
        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
        if (node3 != null) node3.stop();

        if (transport1 != null) transport1.stop();
        if (transport2 != null) transport2.stop();
        if (transport3 != null) transport3.stop();

        if (storage1 != null) storage1.close();
        if (storage2 != null) storage2.close();
        if (storage3 != null) storage3.close();

        InMemoryTransport.clearRegistry();
    }

    @Test
    @DisplayName("[US007 & US008] Committed data and consensus metadata survive crash and restart")
    void testClusterCrashAndRestartPersistence() throws Exception {
        // --- 1. Start Initial Cluster with StorageEngine Attached ---
        Path data1 = clusterDataDir.resolve("node-1");
        Path data2 = clusterDataDir.resolve("node-2");
        Path data3 = clusterDataDir.resolve("node-3");

        storage1 = new StorageEngine(data1);
        storage2 = new StorageEngine(data2);
        storage3 = new StorageEngine(data3);

        transport1 = new InMemoryTransport(id1);
        transport2 = new InMemoryTransport(id2);
        transport3 = new InMemoryTransport(id3);
        transport1.start();
        transport2.start();
        transport3.start();

        StorageEngine.DurableRecovery rec1 = storage1.recoverAndCreateLog();
        PersistentRaftState pState1 = new PersistentRaftState(rec1.result().recoveredTerm(), rec1.result().recoveredVotedFor());
        pState1.setPersistenceListener((term, votedFor) -> {
            try { storage1.metadataStorage().save(term, votedFor); } catch (IOException ignored) {}
        });

        StorageEngine.DurableRecovery rec2 = storage2.recoverAndCreateLog();
        PersistentRaftState pState2 = new PersistentRaftState(rec2.result().recoveredTerm(), rec2.result().recoveredVotedFor());
        pState2.setPersistenceListener((term, votedFor) -> {
            try { storage2.metadataStorage().save(term, votedFor); } catch (IOException ignored) {}
        });

        StorageEngine.DurableRecovery rec3 = storage3.recoverAndCreateLog();
        PersistentRaftState pState3 = new PersistentRaftState(rec3.result().recoveredTerm(), rec3.result().recoveredVotedFor());
        pState3.setPersistenceListener((term, votedFor) -> {
            try { storage3.metadataStorage().save(term, votedFor); } catch (IOException ignored) {}
        });

        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .persistentState(pState1).raftLog(rec1.raftLog())
                .minElectionTimeout(Duration.ofMillis(100)).maxElectionTimeout(Duration.ofMillis(140))
                .heartbeatInterval(Duration.ofMillis(30)).random(new Random(10)).build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .persistentState(pState2).raftLog(rec2.raftLog())
                .minElectionTimeout(Duration.ofMillis(800)).maxElectionTimeout(Duration.ofMillis(1000))
                .heartbeatInterval(Duration.ofMillis(30)).random(new Random(20)).build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .persistentState(pState3).raftLog(rec3.raftLog())
                .minElectionTimeout(Duration.ofMillis(800)).maxElectionTimeout(Duration.ofMillis(1000))
                .heartbeatInterval(Duration.ofMillis(30)).random(new Random(30)).build();

        node1.start();
        node2.start();
        node3.start();

        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);
        long initialLeaderTerm = node1.currentTerm();
        assertThat(initialLeaderTerm).isGreaterThanOrEqualTo(1L);

        // --- 2. Commit Entries via Leader ---
        byte[] cmd1 = "SET account:101 5000".getBytes(StandardCharsets.UTF_8);
        byte[] cmd2 = "SET account:102 3000".getBytes(StandardCharsets.UTF_8);
        byte[] cmd3 = "SET account:103 7000".getBytes(StandardCharsets.UTF_8);

        CompletableFuture<Long> fut1 = node1.propose(cmd1);
        CompletableFuture<Long> fut2 = node1.propose(cmd2);
        CompletableFuture<Long> fut3 = node1.propose(cmd3);

        assertThat(fut1.get(3, TimeUnit.SECONDS)).isEqualTo(1L);
        assertThat(fut2.get(3, TimeUnit.SECONDS)).isEqualTo(2L);
        assertThat(fut3.get(3, TimeUnit.SECONDS)).isEqualTo(3L);

        await().atMost(Duration.ofSeconds(3)).until(() -> node2.commitIndex() == 3L && node3.commitIndex() == 3L);
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node2.log(), 3L);
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), 3L);

        // --- 3. Simulate Abrupt Crash of Entire Cluster ---
        stopCluster();

        // --- 4. Restart Entire Cluster from Disk Storage ---
        StorageEngine restartedStorage1 = new StorageEngine(data1);
        StorageEngine restartedStorage2 = new StorageEngine(data2);
        StorageEngine restartedStorage3 = new StorageEngine(data3);

        // Recover node 1
        StorageEngine.DurableRecovery restartRec1 = restartedStorage1.recoverAndCreateLog();
        assertThat(restartRec1.result().recoveredTerm()).isEqualTo(initialLeaderTerm);
        assertThat(restartRec1.result().lastLogIndex()).isEqualTo(3L);
        assertThat(restartRec1.result().replayedEntries()).hasSize(3);

        // Recover node 2
        StorageEngine.DurableRecovery restartRec2 = restartedStorage2.recoverAndCreateLog();
        assertThat(restartRec2.result().recoveredTerm()).isEqualTo(initialLeaderTerm);
        assertThat(restartRec2.result().lastLogIndex()).isEqualTo(3L);

        // Recover node 3
        StorageEngine.DurableRecovery restartRec3 = restartedStorage3.recoverAndCreateLog();
        assertThat(restartRec3.result().recoveredTerm()).isEqualTo(initialLeaderTerm);
        assertThat(restartRec3.result().lastLogIndex()).isEqualTo(3L);

        // Verify all recovered records match
        assertThat(new String(restartRec1.raftLog().getEntry(1).get().data(), StandardCharsets.UTF_8)).isEqualTo("SET account:101 5000");
        assertThat(new String(restartRec1.raftLog().getEntry(2).get().data(), StandardCharsets.UTF_8)).isEqualTo("SET account:102 3000");
        assertThat(new String(restartRec1.raftLog().getEntry(3).get().data(), StandardCharsets.UTF_8)).isEqualTo("SET account:103 7000");

        // --- 5. Start Cluster Again and Continue Committing New Data ---
        InMemoryTransport newTransport1 = new InMemoryTransport(id1);
        InMemoryTransport newTransport2 = new InMemoryTransport(id2);
        InMemoryTransport newTransport3 = new InMemoryTransport(id3);
        newTransport1.start();
        newTransport2.start();
        newTransport3.start();

        PersistentRaftState newPState1 = new PersistentRaftState(restartRec1.result().recoveredTerm(), restartRec1.result().recoveredVotedFor());
        newPState1.setPersistenceListener((term, votedFor) -> {
            try { restartedStorage1.metadataStorage().save(term, votedFor); } catch (IOException ignored) {}
        });

        PersistentRaftState newPState2 = new PersistentRaftState(restartRec2.result().recoveredTerm(), restartRec2.result().recoveredVotedFor());
        newPState2.setPersistenceListener((term, votedFor) -> {
            try { restartedStorage2.metadataStorage().save(term, votedFor); } catch (IOException ignored) {}
        });

        PersistentRaftState newPState3 = new PersistentRaftState(restartRec3.result().recoveredTerm(), restartRec3.result().recoveredVotedFor());
        newPState3.setPersistenceListener((term, votedFor) -> {
            try { restartedStorage3.metadataStorage().save(term, votedFor); } catch (IOException ignored) {}
        });

        RaftNode restartedNode1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(newTransport1)
                .persistentState(newPState1).raftLog(restartRec1.raftLog())
                .minElectionTimeout(Duration.ofMillis(100)).maxElectionTimeout(Duration.ofMillis(140))
                .heartbeatInterval(Duration.ofMillis(30)).random(new Random(10)).build();

        RaftNode restartedNode2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(newTransport2)
                .persistentState(newPState2).raftLog(restartRec2.raftLog())
                .minElectionTimeout(Duration.ofMillis(800)).maxElectionTimeout(Duration.ofMillis(1000))
                .heartbeatInterval(Duration.ofMillis(30)).random(new Random(20)).build();

        RaftNode restartedNode3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(newTransport3)
                .persistentState(newPState3).raftLog(restartRec3.raftLog())
                .minElectionTimeout(Duration.ofMillis(800)).maxElectionTimeout(Duration.ofMillis(1000))
                .heartbeatInterval(Duration.ofMillis(30)).random(new Random(30)).build();

        restartedNode1.start();
        restartedNode2.start();
        restartedNode3.start();

        await().atMost(Duration.ofSeconds(3)).until(() -> restartedNode1.role() == RaftRole.LEADER);

        // Commit 4th record after restart
        CompletableFuture<Long> fut4 = restartedNode1.propose("SET account:104 9000".getBytes(StandardCharsets.UTF_8));
        assertThat(fut4.get(3, TimeUnit.SECONDS)).isEqualTo(4L);

        await().atMost(Duration.ofSeconds(3)).until(() -> restartedNode2.commitIndex() == 4L && restartedNode3.commitIndex() == 4L);

        RaftInvariants.assertIdenticalOrderOfCommittedEntries(restartedNode1.log(), restartedNode2.log(), 4L);
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(restartedNode1.log(), restartedNode3.log(), 4L);

        // Cleanup
        restartedNode1.stop();
        restartedNode2.stop();
        restartedNode3.stop();
        newTransport1.stop();
        newTransport2.stop();
        newTransport3.stop();
        restartedStorage1.close();
        restartedStorage2.close();
        restartedStorage3.close();
    }
}
