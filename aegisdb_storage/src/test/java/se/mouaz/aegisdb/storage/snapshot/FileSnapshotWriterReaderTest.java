package se.mouaz.aegisdb.storage.snapshot;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.storage.wal.FsyncPolicy;
import se.mouaz.aegisdb.storage.StorageEngine;
import se.mouaz.aegisdb.storage.recovery.RecoveryResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileSnapshotWriterReaderTest {

    @TempDir
    Path tempDir;

    private Path snapshotDir;
    private FileSnapshotWriter writer;
    private FileSnapshotReader reader;

    @BeforeEach
    void setUp() throws IOException {
        snapshotDir = tempDir.resolve("snapshots");
        Files.createDirectories(snapshotDir);
        writer = new FileSnapshotWriter(snapshotDir);
        reader = new FileSnapshotReader(snapshotDir);
    }

    @Test
    void testWriteAndReadSnapshot() throws IOException {
        byte[] payload = "Hello AegisDB Snapshot State!".getBytes(StandardCharsets.UTF_8);
        SnapshotWriter.SnapshotWriteResult writeResult = writer.writeSnapshot(100L, 5L, payload);

        assertThat(writeResult.lastIncludedIndex()).isEqualTo(100L);
        assertThat(writeResult.lastIncludedTerm()).isEqualTo(5L);
        assertThat(Files.exists(writeResult.path())).isTrue();

        Optional<SnapshotReader.SnapshotReadResult> readOpt = reader.readLatestSnapshot();
        assertThat(readOpt).isPresent();

        SnapshotReader.SnapshotReadResult readResult = readOpt.get();
        assertThat(readResult.metadata().lastIncludedIndex()).isEqualTo(100L);
        assertThat(readResult.metadata().lastIncludedTerm()).isEqualTo(5L);
        assertThat(readResult.data()).isEqualTo(payload);
    }

    @Test
    void testSnapshotRetentionPurgesOlderFiles() throws IOException {
        // Write 4 snapshots
        writer.writeSnapshot(10L, 1L, "Snap 1".getBytes(StandardCharsets.UTF_8));
        writer.writeSnapshot(20L, 1L, "Snap 2".getBytes(StandardCharsets.UTF_8));
        writer.writeSnapshot(30L, 2L, "Snap 3".getBytes(StandardCharsets.UTF_8));
        writer.writeSnapshot(40L, 2L, "Snap 4".getBytes(StandardCharsets.UTF_8));

        List<SnapshotReader.SnapshotMetadata> all = reader.listSnapshots();
        // Configured retention is 2 latest snapshots
        assertThat(all).hasSize(2);
        assertThat(all.get(0).lastIncludedIndex()).isEqualTo(40L);
        assertThat(all.get(1).lastIncludedIndex()).isEqualTo(30L);

        // Verify latest read gives the newest one
        Optional<SnapshotReader.SnapshotReadResult> latest = reader.readLatestSnapshot();
        assertThat(latest).isPresent();
        assertThat(latest.get().metadata().lastIncludedIndex()).isEqualTo(40L);
        assertThat(new String(latest.get().data(), StandardCharsets.UTF_8)).isEqualTo("Snap 4");
    }

    @Test
    void testCorruptSnapshotChecksumThrowsException() throws IOException {
        byte[] payload = "Critical database state".getBytes(StandardCharsets.UTF_8);
        SnapshotWriter.SnapshotWriteResult result = writer.writeSnapshot(50L, 3L, payload);

        // Corrupt a byte in payload
        byte[] fileBytes = Files.readAllBytes(result.path());
        fileBytes[fileBytes.length - 1] ^= 0xFF; // flip bits
        Files.write(result.path(), fileBytes);

        // Reading should fail with CorruptedWalException due to CRC32 mismatch
        assertThatThrownBy(() -> reader.readLatestSnapshot())
                .isInstanceOf(se.mouaz.aegisdb.storage.wal.CorruptedWalException.class)
                .hasMessageContaining("checksum mismatch");
    }

    @Test
    void testStorageEngineIntegrationWithSnapshotRecovery() throws IOException {
        Path baseDir = tempDir.resolve("node-data");

        // 1. Write some data and save a snapshot in StorageEngine
        try (StorageEngine engine = new StorageEngine(baseDir, FsyncPolicy.ALWAYS, 1024 * 1024)) {
            engine.metadataStorage().save(3L, se.mouaz.aegisdb.common.NodeId.of("node-1"));
            engine.saveSnapshot(10L, 2L, "KEY:VAL:A=1;B=2".getBytes(StandardCharsets.UTF_8));
        }

        // 2. Recover using a fresh StorageEngine instance
        try (StorageEngine engine2 = new StorageEngine(baseDir, FsyncPolicy.ALWAYS, 1024 * 1024)) {
            StorageEngine.DurableRecovery recovery = engine2.recoverAndCreateLog();
            RecoveryResult res = recovery.result();

            assertThat(res.recoveredTerm()).isEqualTo(3L);
            assertThat(res.snapshotIndex()).isEqualTo(10L);
            assertThat(res.snapshotTerm()).isEqualTo(2L);
            assertThat(res.snapshotData()).isNotNull();
            assertThat(new String(res.snapshotData(), StandardCharsets.UTF_8)).isEqualTo("KEY:VAL:A=1;B=2");

            // Verify DurableRaftLog starts with snapshotIndex = 10
            assertThat(recovery.raftLog().snapshotIndex()).isEqualTo(10L);
            assertThat(recovery.raftLog().snapshotTerm()).isEqualTo(2L);
            assertThat(recovery.raftLog().lastLogIndex()).isEqualTo(10L);
        }
    }
}
