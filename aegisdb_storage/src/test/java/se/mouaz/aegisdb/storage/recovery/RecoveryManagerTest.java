package se.mouaz.aegisdb.storage.recovery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.storage.StorageEngine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class RecoveryManagerTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("RecoveryManager orchestrates complete recovery of metadata and log entries")
    void testFullRecoverySequence() throws IOException {
        NodeId candidateId = NodeId.of("node-2");

        // 1. Initial run: write metadata and log entries
        try (StorageEngine engine = new StorageEngine(tempDir)) {
            engine.metadataStorage().save(3L, candidateId);

            StorageEngine.DurableRecovery recovery = engine.recoverAndCreateLog();
            recovery.raftLog().append(new se.mouaz.aegisdb.raft.log.RaftLogEntry(1L, 1L, "ENTRY-1".getBytes(StandardCharsets.UTF_8)));
            recovery.raftLog().append(new se.mouaz.aegisdb.raft.log.RaftLogEntry(2L, 2L, "ENTRY-2".getBytes(StandardCharsets.UTF_8)));
            recovery.raftLog().append(new se.mouaz.aegisdb.raft.log.RaftLogEntry(3L, 3L, "ENTRY-3".getBytes(StandardCharsets.UTF_8)));
        }

        // 2. Simulate node restart: open new StorageEngine on same directory
        try (StorageEngine restartedEngine = new StorageEngine(tempDir)) {
            StorageEngine.DurableRecovery recovery = restartedEngine.recoverAndCreateLog();
            RecoveryResult result = recovery.result();

            assertThat(result.recoveredTerm()).isEqualTo(3L);
            assertThat(result.recoveredVotedFor()).isEqualTo(candidateId);
            assertThat(result.lastLogIndex()).isEqualTo(3L);
            assertThat(result.lastLogTerm()).isEqualTo(3L);
            assertThat(result.replayedEntries()).hasSize(3);

            // Verify the recovered DurableRaftLog is initialized and usable
            assertThat(recovery.raftLog().lastLogIndex()).isEqualTo(3L);
            assertThat(recovery.raftLog().lastLogTerm()).isEqualTo(3L);

            // Can continue appending from index 4
            recovery.raftLog().append(new se.mouaz.aegisdb.raft.log.RaftLogEntry(4L, 3L, "ENTRY-4".getBytes(StandardCharsets.UTF_8)));
            assertThat(recovery.raftLog().lastLogIndex()).isEqualTo(4L);
        }
    }
}
