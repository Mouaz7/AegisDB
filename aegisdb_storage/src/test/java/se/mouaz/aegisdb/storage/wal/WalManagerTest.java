package se.mouaz.aegisdb.storage.wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class WalManagerTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("WalManager rolls over to new segment when size limit is exceeded")
    void testSegmentRollover() throws IOException {
        // Configure small segment size (200 bytes) to force rollover quickly
        WalConfig config = WalConfig.builder()
                .walDir(tempDir)
                .maxSegmentSizeBytes(200)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        StorageIndex storageIndex = new StorageIndex();

        try (WalManager walManager = new WalManager(config)) {
            for (int i = 1; i <= 10; i++) {
                StorageRecord rec = StorageRecord.createEntry(
                        i,
                        1L,
                        System.currentTimeMillis(),
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("large-payload-value-for-testing-rollover-" + i).getBytes(StandardCharsets.UTF_8)
                );
                StorageIndex.IndexEntry entry = walManager.append(rec);
                storageIndex.put(entry.sequenceNumber(), entry.segmentId(), entry.fileOffset(), entry.recordSize(), entry.term());
            }

            // Must have rolled into multiple segments
            assertThat(walManager.listSegments().size()).isGreaterThan(1);
            assertThat(walManager.activeSegment().segmentId()).isGreaterThan(1L);
        }

        // Reopen WalManager from same directory and verify discovery
        try (WalManager reopened = new WalManager(config)) {
            assertThat(reopened.listSegments().size()).isGreaterThan(1);
            assertThat(reopened.totalWalSize()).isGreaterThan(0);
        }
    }

    @Test
    @DisplayName("WalManager truncates newer segments when repairing conflicting uncommitted entries")
    void testWalTruncation() throws IOException {
        WalConfig config = WalConfig.builder()
                .walDir(tempDir)
                .maxSegmentSizeBytes(150)
                .fsyncPolicy(FsyncPolicy.ALWAYS)
                .build();

        StorageIndex storageIndex = new StorageIndex();

        try (WalManager walManager = new WalManager(config)) {
            for (int i = 1; i <= 8; i++) {
                StorageRecord rec = StorageRecord.createEntry(
                        i,
                        1L,
                        System.currentTimeMillis(),
                        ("val-" + i).getBytes(StandardCharsets.UTF_8)
                );
                StorageIndex.IndexEntry entry = walManager.append(rec);
                storageIndex.put(entry.sequenceNumber(), entry.segmentId(), entry.fileOffset(), entry.recordSize(), entry.term());
            }

            int initialSegmentCount = walManager.listSegments().size();
            assertThat(initialSegmentCount).isGreaterThan(1);

            // Truncate from index 3
            walManager.truncateFrom(3L, storageIndex);

            assertThat(storageIndex.contains(3L)).isFalse();
            assertThat(storageIndex.contains(2L)).isTrue();
            assertThat(storageIndex.lastSequenceNumber()).isEqualTo(2L);
        }
    }
}
