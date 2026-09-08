package se.mouaz.aegisdb.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.storage.wal.CorruptedWalException;
import se.mouaz.aegisdb.storage.wal.SegmentReader;
import se.mouaz.aegisdb.storage.wal.StorageRecord;
import se.mouaz.aegisdb.storage.wal.WalSegment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates Secure-by-Design controls specified in Section 12 of Master Project Plan:
 * Path traversal prevention and bounded allocation limits.
 */
class StorageSecurityTest {

    @Test
    @DisplayName("StorageEngine rejects path traversal attempts in data directory configuration")
    void testPathTraversalRejection() {
        Path maliciousPath = Path.of("data/../../etc/passwd");
        assertThatThrownBy(() -> StorageEngine.validateDataDir(maliciousPath))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Unsafe path traversal");
    }

    @Test
    @DisplayName("SegmentReader rejects crafted records claiming size beyond safety bounds (16MB)")
    void testOversizedRecordFramingRejection() throws IOException {
        Path tempDir = Files.createTempDirectory("aegisdb-security-test");
        try {
            WalSegment segment = WalSegment.openOrCreate(tempDir, 1L);

            // Write a record claiming length of 50MB (exceeding MAX_RECORD_SIZE 16MB)
            ByteBuffer crafted = ByteBuffer.allocate(StorageRecord.FRAMING_HEADER_SIZE);
            crafted.putInt(StorageRecord.MAGIC_NUMBER);
            crafted.putShort(StorageRecord.CURRENT_VERSION);
            crafted.put(StorageRecord.TYPE_DATA);
            crafted.putInt(50 * 1024 * 1024); // 50 MB
            crafted.flip();

            try (FileChannel ch = segment.openChannel(StandardOpenOption.WRITE)) {
                ch.write(crafted);
                ch.force(true);
            }

            try (SegmentReader reader = new SegmentReader(segment)) {
                assertThatThrownBy(reader::readNext)
                        .isInstanceOf(CorruptedWalException.class)
                        .hasMessageContaining("exceeds safety bounds");
            }
        } finally {
            // cleanup
            Files.walk(tempDir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
        }
    }
}
