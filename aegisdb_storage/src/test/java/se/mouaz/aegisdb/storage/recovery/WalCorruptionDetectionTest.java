package se.mouaz.aegisdb.storage.recovery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.storage.wal.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies Acceptance Criterion 5 (AC5): Corruption detection (Master Project Plan §17, US007).
 * When corruption occurs within the WAL (CRC32 mismatch, invalid magic, corrupted version),
 * the storage engine detects it and throws CorruptedWalException.
 */
class WalCorruptionDetectionTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("[AC5] Detects CRC32 checksum mismatch on corrupted record payload")
    void testDetectsChecksumCorruption() throws IOException {
        WalConfig config = WalConfig.of(tempDir);

        try (WalManager walManager = new WalManager(config)) {
            walManager.openWriter();
            walManager.append(StorageRecord.createEntry(
                    1L, 1L, System.currentTimeMillis(), "account:1".getBytes(StandardCharsets.UTF_8), "1000".getBytes(StandardCharsets.UTF_8)
            ));
            walManager.append(StorageRecord.createEntry(
                    2L, 1L, System.currentTimeMillis(), "account:2".getBytes(StandardCharsets.UTF_8), "2000".getBytes(StandardCharsets.UTF_8)
            ));
        }

        // Corrupt a byte in the middle of the first record's payload
        WalSegment segment = WalSegment.openOrCreate(tempDir, 1L);
        byte[] fileBytes = Files.readAllBytes(segment.path());
        fileBytes[StorageRecord.FRAMING_HEADER_SIZE + 20] ^= 0x42; // flip bits in payload
        Files.write(segment.path(), fileBytes);

        try (WalManager walManager = new WalManager(config)) {
            walManager.openWriter();
            WalRecoveryManager recoveryManager = new WalRecoveryManager(walManager);
            assertThatThrownBy(recoveryManager::scanAndRecover)
                    .isInstanceOf(CorruptedWalException.class)
                    .hasMessageContaining("CRC32 Checksum mismatch");
        }
    }

    @Test
    @DisplayName("[AC5] Detects invalid magic number in WAL record header")
    void testDetectsInvalidMagic() throws IOException {
        WalConfig config = WalConfig.of(tempDir);

        try (WalManager walManager = new WalManager(config)) {
            walManager.openWriter();
            walManager.append(StorageRecord.createEntry(1L, 1L, System.currentTimeMillis(), "data".getBytes(StandardCharsets.UTF_8)));
        }

        WalSegment segment = WalSegment.openOrCreate(tempDir, 1L);
        byte[] fileBytes = Files.readAllBytes(segment.path());
        fileBytes[0] = (byte) 0xFF; // corrupt magic number
        Files.write(segment.path(), fileBytes);

        try (WalManager walManager = new WalManager(config)) {
            walManager.openWriter();
            WalRecoveryManager recoveryManager = new WalRecoveryManager(walManager);
            assertThatThrownBy(recoveryManager::scanAndRecover)
                    .isInstanceOf(CorruptedWalException.class)
                    .hasMessageContaining("Invalid WAL magic number");
        }
    }
}
