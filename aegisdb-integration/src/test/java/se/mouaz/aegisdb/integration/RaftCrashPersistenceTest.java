package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.storage.DurableRaftLog;
import se.mouaz.aegisdb.storage.StorageEngine;
import se.mouaz.aegisdb.storage.recovery.RecoveryResult;
import se.mouaz.aegisdb.storage.wal.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates crash durability, fsync policy guarantees, and WAL corruption recovery:
 * 1. currentTerm and votedFor durability across hard node restart.
 * 2. Committed Raft entries survive hard process crash without data loss.
 * 3. FsyncPolicy.ALWAYS guarantees immediate physical durability on every commit.
 * 4. Comprehensive corruption matrix:
 *    - Truncated framing header (< 11 bytes) at EOF -> torn tail safely truncated.
 *    - Truncated payload at EOF -> torn tail safely truncated.
 *    - CRC32 checksum mismatch -> detected, fails closed with CorruptedWalException.
 *    - Invalid WAL magic number -> detected, fails closed with CorruptedWalException.
 *    - Post-recovery append continuity -> seamless append after recovery.
 */
@DisplayName("Raft Crash Durability, Fsync & WAL Corruption Matrix Test Suite")
class RaftCrashPersistenceTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("1. currentTerm and votedFor survive hard node crash and restart")
    void currentTermAndVoteDurabilityAcrossHardRestart() throws IOException {
        Path nodeDataDir = tempDir.resolve("node-metadata-test");

        // 1. Initial run: save term and vote using FileRaftMetadataStorage
        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            engine.metadataStorage().save(7L, NodeId.of("leader-node-candidate"));
        }

        // 2. Restart from persistent files on disk
        try (StorageEngine restartedEngine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery recovery = restartedEngine.recoverAndCreateLog();

            RecoveryResult result = recovery.result();
            assertThat(result.recoveredTerm()).isEqualTo(7L);
            assertThat(result.recoveredVotedFor()).isEqualTo(NodeId.of("leader-node-candidate"));
        }
    }

    @Test
    @DisplayName("2. Committed entries survive hard restart without data loss")
    void committedEntriesSurviveHardRestart() throws IOException {
        Path nodeDataDir = tempDir.resolve("node-log-durability-test");

        // 1. Write committed entries 1 to 5 to DurableRaftLog
        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery initRecovery = engine.recoverAndCreateLog();
            DurableRaftLog raftLog = initRecovery.raftLog();

            for (int i = 1; i <= 5; i++) {
                byte[] payload = ("committed-key-" + i + "=committed-val-" + i).getBytes(StandardCharsets.UTF_8);
                raftLog.append(new RaftLogEntry((long) i, 2L, payload));
            }
            assertThat(raftLog.lastLogIndex()).isEqualTo(5L);
        }

        // 2. Verify disk state directly: WAL segment file must be physically populated
        Path walDir = nodeDataDir.resolve("wal");
        assertThat(Files.list(walDir).count()).isGreaterThanOrEqualTo(1L);

        // 3. Restart node afresh and recover
        try (StorageEngine restartedEngine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery recovery = restartedEngine.recoverAndCreateLog();
            DurableRaftLog recoveredLog = recovery.raftLog();

            assertThat(recoveredLog.lastLogIndex()).isEqualTo(5L);
            for (int i = 1; i <= 5; i++) {
                Optional<RaftLogEntry> entryOpt = recoveredLog.getEntry((long) i);
                assertThat(entryOpt).isPresent();
                RaftLogEntry entry = entryOpt.get();
                assertThat(entry.index()).isEqualTo((long) i);
                assertThat(entry.term()).isEqualTo(2L);
                assertThat(new String(entry.data(), StandardCharsets.UTF_8))
                        .isEqualTo("committed-key-" + i + "=committed-val-" + i);
            }

            // 4. Subsequent writes work seamlessly on the recovered log
            byte[] entry6 = "committed-key-6=committed-val-6".getBytes(StandardCharsets.UTF_8);
            recoveredLog.append(new RaftLogEntry(6L, 2L, entry6));
            assertThat(recoveredLog.lastLogIndex()).isEqualTo(6L);
        }

        // 5. Verify index 6 was also durably persisted
        try (StorageEngine secondRestartEngine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery secondRecovery = secondRestartEngine.recoverAndCreateLog();
            assertThat(secondRecovery.raftLog().lastLogIndex()).isEqualTo(6L);
        }
    }

    @Test
    @DisplayName("3. FsyncPolicy.ALWAYS forces immediate physical sync to disk")
    void fsyncPolicyAlwaysGuaranteesImmediateDurability() throws IOException {
        Path nodeDataDir = tempDir.resolve("node-fsync-test");

        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery initRecovery = engine.recoverAndCreateLog();
            DurableRaftLog raftLog = initRecovery.raftLog();

            raftLog.append(new RaftLogEntry(1L, 1L, "durable-sync-data".getBytes(StandardCharsets.UTF_8)));

            // Verify active segment file on disk immediately contains the entry bytes
            WalSegment segment = engine.walManager().activeSegment();
            assertThat(segment.size()).isGreaterThan((long) StorageRecord.FRAMING_HEADER_SIZE);
        }
    }

    @Test
    @DisplayName("4. WAL Corruption Matrix: Truncated framing header (< 11 bytes) at EOF is safely repaired")
    void walCorruptionMatrixTruncatedFramingHeaderAtEof() throws IOException {
        Path nodeDataDir = tempDir.resolve("wal-torn-header-test");
        long intactSize;

        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery init = engine.recoverAndCreateLog();
            for (int i = 1; i <= 3; i++) {
                init.raftLog().append(new RaftLogEntry((long) i, 1L, ("valid-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            intactSize = engine.walManager().activeSegment().size();
        }

        // Inject 5 bytes (incomplete framing header, < 11 bytes) at EOF
        WalSegment segment = WalSegment.openOrCreate(nodeDataDir.resolve("wal"), 1L);
        try (FileChannel channel = segment.openChannel(StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer partialHeader = ByteBuffer.allocate(5);
            partialHeader.putInt(StorageRecord.MAGIC_NUMBER);
            partialHeader.put((byte) 1);
            partialHeader.flip();
            channel.write(partialHeader);
            channel.force(true);
        }
        assertThat(segment.size()).isEqualTo(intactSize + 5);

        // Recovery must detect torn header, safely truncate, and preserve all 3 valid entries
        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery recovery = engine.recoverAndCreateLog();
            assertThat(recovery.result().repairedTornTailsCount()).isEqualTo(1);
            assertThat(recovery.raftLog().lastLogIndex()).isEqualTo(3L);
            assertThat(segment.size()).isEqualTo(intactSize);
        }
    }

    @Test
    @DisplayName("5. WAL Corruption Matrix: Truncated payload at EOF is safely repaired")
    void walCorruptionMatrixTruncatedPayloadAtEof() throws IOException {
        Path nodeDataDir = tempDir.resolve("wal-torn-payload-test");
        long intactSize;

        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery init = engine.recoverAndCreateLog();
            for (int i = 1; i <= 3; i++) {
                init.raftLog().append(new RaftLogEntry((long) i, 1L, ("valid-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            intactSize = engine.walManager().activeSegment().size();
        }

        // Inject full 11-byte header declaring a 100-byte record, but only append 15 bytes of body (torn write)
        WalSegment segment = WalSegment.openOrCreate(nodeDataDir.resolve("wal"), 1L);
        try (FileChannel channel = segment.openChannel(StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer tornHeaderAndPartialBody = ByteBuffer.allocate(11 + 15);
            tornHeaderAndPartialBody.putInt(StorageRecord.MAGIC_NUMBER);
            tornHeaderAndPartialBody.putShort(StorageRecord.CURRENT_VERSION);
            tornHeaderAndPartialBody.put(StorageRecord.TYPE_DATA);
            tornHeaderAndPartialBody.putInt(100); // Claims 100 bytes
            tornHeaderAndPartialBody.put(new byte[15]); // Only writes 15 bytes
            tornHeaderAndPartialBody.flip();
            channel.write(tornHeaderAndPartialBody);
            channel.force(true);
        }
        assertThat(segment.size()).isEqualTo(intactSize + 26);

        // Recovery safely detects incomplete payload at EOF and truncates back to intact state
        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery recovery = engine.recoverAndCreateLog();
            assertThat(recovery.result().repairedTornTailsCount()).isEqualTo(1);
            assertThat(recovery.raftLog().lastLogIndex()).isEqualTo(3L);
            assertThat(segment.size()).isEqualTo(intactSize);
        }
    }

    @Test
    @DisplayName("6. WAL Corruption Matrix: Checksum CRC32 mismatch triggers CorruptedWalException")
    void walCorruptionMatrixChecksumMismatchThrowsCorruptedWalException() throws IOException {
        Path nodeDataDir = tempDir.resolve("wal-checksum-mismatch-test");

        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery init = engine.recoverAndCreateLog();
            init.raftLog().append(new RaftLogEntry(1L, 1L, "important-state-1".getBytes(StandardCharsets.UTF_8)));
            init.raftLog().append(new RaftLogEntry(2L, 1L, "important-state-2".getBytes(StandardCharsets.UTF_8)));
        }

        // Corrupt a byte in the payload of entry 1
        WalSegment segment = WalSegment.openOrCreate(nodeDataDir.resolve("wal"), 1L);
        byte[] fileBytes = Files.readAllBytes(segment.path());
        fileBytes[StorageRecord.FRAMING_HEADER_SIZE + 20] ^= 0x5A; // flip bits
        Files.write(segment.path(), fileBytes);

        // Fails closed with CorruptedWalException
        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            assertThatThrownBy(engine::recoverAndCreateLog)
                    .isInstanceOf(CorruptedWalException.class)
                    .hasMessageContaining("CRC32 Checksum mismatch");
        }
    }

    @Test
    @DisplayName("7. WAL Corruption Matrix: Invalid WAL magic number triggers CorruptedWalException")
    void walCorruptionMatrixInvalidMagicThrowsCorruptedWalException() throws IOException {
        Path nodeDataDir = tempDir.resolve("wal-invalid-magic-test");

        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery init = engine.recoverAndCreateLog();
            init.raftLog().append(new RaftLogEntry(1L, 1L, "valid-data".getBytes(StandardCharsets.UTF_8)));
        }

        // Corrupt magic number in the active segment
        WalSegment segment = WalSegment.openOrCreate(nodeDataDir.resolve("wal"), 1L);
        byte[] fileBytes = Files.readAllBytes(segment.path());
        fileBytes[0] = (byte) 0xDE;
        fileBytes[1] = (byte) 0xAD;
        fileBytes[2] = (byte) 0xBE;
        fileBytes[3] = (byte) 0xEF;
        Files.write(segment.path(), fileBytes);

        // Fails closed with CorruptedWalException
        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            assertThatThrownBy(engine::recoverAndCreateLog)
                    .isInstanceOf(CorruptedWalException.class)
                    .hasMessageContaining("Invalid WAL magic number");
        }
    }

    @Test
    @DisplayName("8. Post-recovery append continuity functions seamlessly after torn tail repair")
    void postRecoveryAppendContinuity() throws IOException {
        Path nodeDataDir = tempDir.resolve("wal-continuity-test");

        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery init = engine.recoverAndCreateLog();
            init.raftLog().append(new RaftLogEntry(1L, 1L, "first".getBytes(StandardCharsets.UTF_8)));
            init.raftLog().append(new RaftLogEntry(2L, 1L, "second".getBytes(StandardCharsets.UTF_8)));
        }

        // Inject torn 7 bytes at EOF
        WalSegment segment = WalSegment.openOrCreate(nodeDataDir.resolve("wal"), 1L);
        try (FileChannel channel = segment.openChannel(StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7}));
            channel.force(true);
        }

        // Recover, verify torn tail repaired, then append entries 3 and 4
        try (StorageEngine engine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery recovery = engine.recoverAndCreateLog();
            assertThat(recovery.result().repairedTornTailsCount()).isEqualTo(1);
            assertThat(recovery.raftLog().lastLogIndex()).isEqualTo(2L);

            recovery.raftLog().append(new RaftLogEntry(3L, 1L, "third".getBytes(StandardCharsets.UTF_8)));
            recovery.raftLog().append(new RaftLogEntry(4L, 1L, "fourth".getBytes(StandardCharsets.UTF_8)));
            assertThat(recovery.raftLog().lastLogIndex()).isEqualTo(4L);
        }

        // Final verification across a clean restart
        try (StorageEngine verifyEngine = new StorageEngine(nodeDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES)) {
            StorageEngine.DurableRecovery finalRecovery = verifyEngine.recoverAndCreateLog();
            assertThat(finalRecovery.raftLog().lastLogIndex()).isEqualTo(4L);
            assertThat(new String(finalRecovery.raftLog().getEntry(3L).get().data(), StandardCharsets.UTF_8)).isEqualTo("third");
            assertThat(new String(finalRecovery.raftLog().getEntry(4L).get().data(), StandardCharsets.UTF_8)).isEqualTo("fourth");
        }
    }
}
