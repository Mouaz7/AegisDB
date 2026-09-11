package se.mouaz.aegisdb.raft.snapshot;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.InstallSnapshotRequest;
import se.mouaz.aegisdb.protocol.InstallSnapshotResponse;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import se.mouaz.aegisdb.raft.statemachine.KvCommand;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class InstallSnapshotHandlerTest {

    private KeyValueStateMachine stateMachine;
    private RaftLog raftLog;
    private SnapshotManager snapshotManager;
    private AtomicLong installedIndex;
    private AtomicBoolean savedToDisk;

    @BeforeEach
    void setUp() {
        stateMachine = new KeyValueStateMachine();
        raftLog = new RaftLog();
        installedIndex = new AtomicLong(-1);
        savedToDisk = new AtomicBoolean(false);

        // Prepopulate some entries in raft log
        for (int i = 1; i <= 5; i++) {
            raftLog.append(new RaftLogEntry(i, 1, ("entry_" + i).getBytes(StandardCharsets.UTF_8)));
        }

        snapshotManager = new SnapshotManager(
                stateMachine,
                raftLog,
                (idx, data) -> savedToDisk.set(true),
                (idx, term) -> installedIndex.set(idx)
        );
    }

    @Test
    void testRejectSnapshotWithLowerTerm() {
        InstallSnapshotRequest req = new InstallSnapshotRequest(
                1L,
                NodeId.of("node-1"),
                10L,
                2L,
                0L,
                new byte[10],
                true
        );

        InstallSnapshotResponse resp = snapshotManager.handleInstallSnapshot(req, 2L); // currentTerm 2 > reqTerm 1
        assertThat(resp.success()).isFalse();
        assertThat(resp.term()).isEqualTo(2L);
        assertThat(installedIndex.get()).isEqualTo(-1);
    }

    @Test
    void testReceiveSnapshotInChunksAndRestoreStateMachine() {
        // Create sample state machine data
        KeyValueStateMachine sourceSm = new KeyValueStateMachine();
        for (int i = 1; i <= 20; i++) {
            sourceSm.apply((long) i, KvCommand.put("key" + i, ("val" + i).getBytes(StandardCharsets.UTF_8)).toBytes());
        }
        byte[] fullSnapshotBytes = sourceSm.takeSnapshot();

        // Split into 2 chunks
        int half = fullSnapshotBytes.length / 2;
        byte[] chunk1 = new byte[half];
        byte[] chunk2 = new byte[fullSnapshotBytes.length - half];
        System.arraycopy(fullSnapshotBytes, 0, chunk1, 0, half);
        System.arraycopy(fullSnapshotBytes, half, chunk2, 0, chunk2.length);

        // Chunk 1
        InstallSnapshotRequest req1 = new InstallSnapshotRequest(
                3L,
                NodeId.of("leader-1"),
                20L,
                3L,
                0L,
                chunk1,
                false
        );
        InstallSnapshotResponse resp1 = snapshotManager.handleInstallSnapshot(req1, 3L);
        assertThat(resp1.success()).isTrue();
        assertThat(installedIndex.get()).isEqualTo(-1); // Not done yet

        // Chunk 2 (done)
        InstallSnapshotRequest req2 = new InstallSnapshotRequest(
                3L,
                NodeId.of("leader-1"),
                20L,
                3L,
                (long) half,
                chunk2,
                true
        );
        InstallSnapshotResponse resp2 = snapshotManager.handleInstallSnapshot(req2, 3L);
        assertThat(resp2.success()).isTrue();
        assertThat(installedIndex.get()).isEqualTo(20L); // Installed!
        assertThat(savedToDisk.get()).isTrue();

        // Verify state machine has restored entries
        assertThat(stateMachine.size()).isEqualTo(20);
        assertThat(new String(stateMachine.get("key1"), StandardCharsets.UTF_8)).isEqualTo("val1");
        assertThat(new String(stateMachine.get("key20"), StandardCharsets.UTF_8)).isEqualTo("val20");

        // Verify log was compacted up to 20
        assertThat(raftLog.snapshotIndex()).isEqualTo(20L);
        assertThat(raftLog.snapshotTerm()).isEqualTo(3L);
        assertThat(raftLog.lastLogIndex()).isEqualTo(20L);
    }
}
