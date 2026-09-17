package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.storage.DurableRaftLog;
import se.mouaz.aegisdb.storage.StorageEngine;
import se.mouaz.aegisdb.storage.recovery.RecoveryResult;
import se.mouaz.aegisdb.storage.wal.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Storage Engine Deterministic Crash Consistency Matrix")
class StorageCrashConsistencyMatrixTest {

    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "Crash point: {0}")
    @EnumSource(value = CrashPoint.class, names = {
            "BEFORE_RECORD_HEADER",
            "PARTIAL_RECORD_HEADER",
            "PARTIAL_PAYLOAD",
            "BEFORE_FSYNC",
            "DURING_FSYNC",
            "AFTER_FSYNC_BEFORE_METADATA_SAVE"
    })
    @DisplayName("Verify recovery consistency across deterministic crash matrix")
    void testCrashRecoveryMatrix(CrashPoint crashPoint) throws IOException {
        Path nodeDir = tempDir.resolve("node-" + crashPoint.name().toLowerCase());

        // 1. Initial Phase: Write 3 fully committed records
        try (StorageEngine engine = new StorageEngine(nodeDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            engine.metadataStorage().save(2L, NodeId.of("leader-node"));
            StorageEngine.DurableRecovery initRecovery = engine.recoverAndCreateLog();
            DurableRaftLog log = initRecovery.raftLog();

            for (long i = 1; i <= 3; i++) {
                byte[] data = ("committed-key-" + i + "=committed-val-" + i).getBytes(StandardCharsets.UTF_8);
                log.append(new RaftLogEntry(i, 2L, data));
            }
            assertThat(log.lastLogIndex()).isEqualTo(3L);
        }

        // 2. Fault Injection Phase: Inject precision crash point into active segment during 4th record write
        Path walDir = nodeDir.resolve("wal");
        WalSegment activeSegment = WalSegment.openOrCreate(walDir, 1L);

        try (CrashableStorageChannel crashableChannel = new CrashableStorageChannel(
                activeSegment.openChannel(StandardOpenOption.WRITE, StandardOpenOption.APPEND))) {

            crashableChannel.arm(crashPoint);

            byte[] data4 = "torn-uncommitted-record-4".getBytes(StandardCharsets.UTF_8);
            StorageRecord uncommittedRecord = StorageRecord.createEntry(4L, 2L, System.currentTimeMillis(), data4);
            ByteBuffer buffer = uncommittedRecord.serialize();

            try {
                crashableChannel.write(buffer);
                crashableChannel.force(true);
            } catch (StorageCrashException e) {
                assertThat(e.crashPoint()).isEqualTo(crashPoint);
            }
        }

        // 3. Recovery Phase: Restart engine from persisted disk state
        try (StorageEngine recoveredEngine = new StorageEngine(nodeDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery recovery = recoveredEngine.recoverAndCreateLog();
            RecoveryResult result = recovery.result();
            DurableRaftLog recoveredLog = recovery.raftLog();

            // Metadata must be intact
            assertThat(result.recoveredTerm()).isEqualTo(2L);
            assertThat(result.recoveredVotedFor()).isEqualTo(NodeId.of("leader-node"));

            // All 3 committed records must be 100% recovered and bit-identical
            boolean isCompleteWrite = crashPoint == CrashPoint.BEFORE_FSYNC
                    || crashPoint == CrashPoint.DURING_FSYNC
                    || crashPoint == CrashPoint.AFTER_FSYNC_BEFORE_METADATA_SAVE;

            long expectedLastIndex = isCompleteWrite ? 4L : 3L;
            assertThat(recoveredLog.lastLogIndex()).isEqualTo(expectedLastIndex);

            for (long i = 1; i <= 3; i++) {
                Optional<RaftLogEntry> entryOpt = recoveredLog.getEntry(i);
                assertThat(entryOpt).isPresent();
                RaftLogEntry entry = entryOpt.get();
                assertThat(entry.index()).isEqualTo(i);
                assertThat(entry.term()).isEqualTo(2L);
                assertThat(new String(entry.data(), StandardCharsets.UTF_8))
                        .isEqualTo("committed-key-" + i + "=committed-val-" + i);
            }

            // 4. Subsequent Continuity: Appending post-recovery succeeds seamlessly
            long nextAppendIndex = expectedLastIndex + 1;
            byte[] validDataNext = ("post-recovery-committed-record-" + nextAppendIndex).getBytes(StandardCharsets.UTF_8);
            recoveredLog.append(new RaftLogEntry(nextAppendIndex, 2L, validDataNext));
            assertThat(recoveredLog.lastLogIndex()).isEqualTo(nextAppendIndex);

            Optional<RaftLogEntry> postRecoveryEntry = recoveredLog.getEntry(nextAppendIndex);
            assertThat(postRecoveryEntry).isPresent();
            assertThat(new String(postRecoveryEntry.get().data(), StandardCharsets.UTF_8))
                    .isEqualTo("post-recovery-committed-record-" + nextAppendIndex);
        }
    }
}
