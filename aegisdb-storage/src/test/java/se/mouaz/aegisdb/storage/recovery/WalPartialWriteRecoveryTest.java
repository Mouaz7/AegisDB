package se.mouaz.aegisdb.storage.recovery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.storage.wal.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies Acceptance Criterion 4 (AC4): Partial-write recovery (Master Project Plan §17, US007).
 * When a crash or power cut interrupts a write mid-record at the tail of the WAL,
 * the recovery manager safely detects the torn tail, truncates it, and preserves all prior valid records.
 */
class WalPartialWriteRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("[AC4] Safe partial-write recovery repairs torn tail at EOF and preserves valid records")
    void testPartialWriteRecoveryAtTail() throws IOException {
        WalConfig config = WalConfig.of(tempDir);
        StorageIndex storageIndex = new StorageIndex();

        // 1. Write 3 valid committed records
        try (WalManager walManager = new WalManager(config)) {
            walManager.openWriter();
            for (int i = 1; i <= 3; i++) {
                StorageRecord rec = StorageRecord.createEntry(
                        i,
                        1L,
                        System.currentTimeMillis(),
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("val-" + i).getBytes(StandardCharsets.UTF_8)
                );
                StorageIndex.IndexEntry entry = walManager.append(rec);
                storageIndex.put(entry.sequenceNumber(), entry.segmentId(), entry.fileOffset(), entry.recordSize(), entry.term());
            }
        }

        // 2. Simulate a crash mid-write: append a torn/incomplete record to the active segment
        WalSegment segment = WalSegment.openOrCreate(tempDir, 1L);
        long intactSize = segment.size();

        StorageRecord tornCandidate = StorageRecord.createEntry(
                4L,
                1L,
                System.currentTimeMillis(),
                "torn-key".getBytes(StandardCharsets.UTF_8),
                "torn-payload-that-was-interrupted-by-abrupt-crash".getBytes(StandardCharsets.UTF_8)
        );
        ByteBuffer serialized = tornCandidate.serialize();

        // Write only half of the serialized buffer (simulating torn write)
        byte[] halfBytes = new byte[serialized.remaining() / 2];
        serialized.get(halfBytes);

        try (FileChannel channel = segment.openChannel(StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            channel.write(ByteBuffer.wrap(halfBytes));
            channel.force(true);
        }

        assertThat(segment.size()).isGreaterThan(intactSize);

        // 3. Run WalRecoveryManager on the crashed WAL directory
        try (WalManager recoveryWalManager = new WalManager(config)) {
            WalRecoveryManager recoveryManager = new WalRecoveryManager(recoveryWalManager);
            WalRecoveryManager.WalScanResult result = recoveryManager.scanAndRecover();

            // Must have recovered exactly 3 records
            assertThat(result.records()).hasSize(3);
            assertThat(result.records().get(0).sequenceNumber()).isEqualTo(1L);
            assertThat(result.records().get(1).sequenceNumber()).isEqualTo(2L);
            assertThat(result.records().get(2).sequenceNumber()).isEqualTo(3L);

            // Must have repaired 1 torn tail
            assertThat(result.tornTailsRepairedCount()).isEqualTo(1);

            // Physical file must have been safely truncated back to intact size
            assertThat(segment.size()).isEqualTo(intactSize);
        }

        // 4. Verify subsequent appends work cleanly on the repaired segment
        try (WalManager resumeManager = new WalManager(config)) {
            resumeManager.openWriter();
            StorageRecord newRecord = StorageRecord.createEntry(
                    4L,
                    1L,
                    System.currentTimeMillis(),
                    "clean-key".getBytes(StandardCharsets.UTF_8),
                    "clean-value".getBytes(StandardCharsets.UTF_8)
            );
            resumeManager.append(newRecord);

            WalRecoveryManager verifyManager = new WalRecoveryManager(resumeManager);
            WalRecoveryManager.WalScanResult finalResult = verifyManager.scanAndRecover();
            assertThat(finalResult.records()).hasSize(4);
            assertThat(finalResult.records().get(3).sequenceNumber()).isEqualTo(4L);
            assertThat(new String(finalResult.records().get(3).key(), StandardCharsets.UTF_8)).isEqualTo("clean-key");
        }
    }
}
