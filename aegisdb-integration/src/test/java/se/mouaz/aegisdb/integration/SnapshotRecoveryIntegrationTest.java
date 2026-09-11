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
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.raft.snapshot.SnapshotManager;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import se.mouaz.aegisdb.raft.statemachine.KvCommand;
import se.mouaz.aegisdb.storage.StorageEngine;
import se.mouaz.aegisdb.storage.snapshot.SnapshotReader;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies Phase 5 & US009 Acceptance Criteria:
 * [AC1] Snapshot metadata, framing, and CRC32 checksum.
 * [AC6] Persistent state recovery: Node recovers from disk snapshot + subsequent WAL entries.
 */
class SnapshotRecoveryIntegrationTest {

    @TempDir
    Path dataDir;

    private NodeId nodeId;
    private ClusterConfiguration clusterConfig;
    private InMemoryTransport transport;
    private StorageEngine storageEngine;
    private RaftNode node;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        nodeId = NodeId.of("recover-node-1");
        Endpoint ep = Endpoint.of("127.0.0.1", 13001);

        clusterConfig = ClusterConfiguration.builder()
                .clusterId("recover-cluster")
                .addMember(nodeId, ep)
                .build();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (node != null) node.stop();
        if (transport != null) transport.stop();
        if (storageEngine != null) storageEngine.close();
        InMemoryTransport.clearRegistry();
    }

    @Test
    @DisplayName("[US009 / AC1 & AC6] Node recovers from disk snapshot and replays subsequent WAL records")
    void testSnapshotAndWalReplayRecovery() throws Exception {
        // --- 1. First Run: Initialize storage and start node ---
        storageEngine = new StorageEngine(dataDir);
        transport = new InMemoryTransport(nodeId);
        transport.start();

        StorageEngine.DurableRecovery recovery1 = storageEngine.recoverAndCreateLog();
        PersistentRaftState pState1 = new PersistentRaftState(
                recovery1.result().recoveredTerm(),
                recovery1.result().recoveredVotedFor()
        );
        pState1.setPersistenceListener((term, votedFor) -> {
            try { storageEngine.metadataStorage().save(term, votedFor); } catch (IOException ignored) {}
        });

        KeyValueStateMachine sm1 = new KeyValueStateMachine();
        SnapshotManager snapMgr1 = new SnapshotManager(
                sm1,
                recovery1.raftLog(),
                (idx, bytes) -> {
                    try {
                        storageEngine.saveSnapshot(idx, 1L, bytes);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                null
        );

        node = RaftNode.builder()
                .nodeId(nodeId)
                .clusterConfig(clusterConfig)
                .transport(transport)
                .persistentState(pState1)
                .raftLog(recovery1.raftLog())
                .stateMachine(sm1)
                .snapshotManager(snapMgr1)
                .minElectionTimeout(Duration.ofMillis(80))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(25))
                .random(new Random(42))
                .build();

        node.start();

        // Node is sole member in cluster, becomes leader immediately
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == RaftRole.LEADER);

        // Execute 10 KV PUT operations (entries 1 to 10)
        for (int i = 1; i <= 10; i++) {
            byte[] cmd = KvCommand.put("snap-key-" + i, ("val-" + i).getBytes(StandardCharsets.UTF_8)).toBytes();
            CompletableFuture<byte[]> f = node.executeClientCommand(cmd);
            f.get(5, TimeUnit.SECONDS);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> sm1.get("snap-key-10") != null);
        assertThat(node.commitIndex()).isEqualTo(10);

        // --- 2. Take Snapshot on disk at index 10 ---
        byte[] snapBytes = node.takeSnapshot(10, node.currentTerm());
        assertThat(snapBytes).isNotEmpty();

        // Verify snapshot file exists on disk
        Optional<SnapshotReader.SnapshotMetadata> snapMeta = storageEngine.snapshotReader().loadLatestSnapshotMetadata();
        assertThat(snapMeta).isPresent();
        assertThat(snapMeta.get().lastIncludedIndex()).isEqualTo(10);

        // --- 3. Execute 5 more KV operations to WAL (entries 11 to 15) ---
        for (int i = 11; i <= 15; i++) {
            byte[] cmd = KvCommand.put("snap-key-" + i, ("val-" + i).getBytes(StandardCharsets.UTF_8)).toBytes();
            CompletableFuture<byte[]> f = node.executeClientCommand(cmd);
            f.get(5, TimeUnit.SECONDS);
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> sm1.get("snap-key-15") != null);
        assertThat(node.commitIndex()).isEqualTo(15);

        // --- 4. Crash Simulation: Stop node and close storage engine ---
        node.stop();
        transport.stop();
        storageEngine.close();
        InMemoryTransport.clearRegistry();

        // --- 5. Recovery Phase: Reopen StorageEngine on same directory ---
        StorageEngine recoveredStorage = new StorageEngine(dataDir);
        try {
            StorageEngine.DurableRecovery recovery2 = recoveredStorage.recoverAndCreateLog();

            // Verify RecoveryManager loaded the snapshot
            assertThat(recovery2.result().hasSnapshot()).isTrue();
            assertThat(recovery2.result().snapshotIndex()).isEqualTo(10);

            // Reconstruct state machine from snapshot + replayed WAL
            KeyValueStateMachine recoveredSm = new KeyValueStateMachine();

            // Step A: Restore snapshot data
            Optional<SnapshotReader.SnapshotReadResult> recoveredSnapResult = recoveredStorage.readLatestSnapshot();
            assertThat(recoveredSnapResult).isPresent();
            recoveredSm.restoreSnapshot(
                    recovery2.result().snapshotIndex(),
                    recoveredSnapResult.get().data()
            );

            // Verify pre-snapshot keys are restored
            for (int i = 1; i <= 10; i++) {
                byte[] v = recoveredSm.get("snap-key-" + i);
                assertThat(v).as("Key snap-key-" + i + " must be present from snapshot").isNotNull();
                assertThat(new String(v, StandardCharsets.UTF_8)).isEqualTo("val-" + i);
            }

            // Step B: Replay post-snapshot WAL entries into state machine
            for (RaftLogEntry entry : recovery2.result().replayedEntries()) {
                if (entry.index() > 10) {
                    recoveredSm.apply(entry.index(), entry.command().payload());
                }
            }

            // Verify post-snapshot keys are restored
            for (int i = 11; i <= 15; i++) {
                byte[] v = recoveredSm.get("snap-key-" + i);
                assertThat(v).as("Key snap-key-" + i + " must be present from replayed WAL").isNotNull();
                assertThat(new String(v, StandardCharsets.UTF_8)).isEqualTo("val-" + i);
            }
        } finally {
            recoveredStorage.close();
        }
    }
}
