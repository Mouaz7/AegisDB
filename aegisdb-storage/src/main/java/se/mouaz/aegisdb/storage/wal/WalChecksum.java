package se.mouaz.aegisdb.storage.wal;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * CRC32 Checksum utility for Write-Ahead Log records and headers (Master Project Plan §8).
 * Detects bit-level corruption and torn writes.
 */
public final class WalChecksum {

    private WalChecksum() {
        // Utility class
    }

    /**
     * Computes the CRC32 checksum over the provided byte array.
     */
    public static long compute(byte[] data) {
        if (data == null || data.length == 0) {
            return 0L;
        }
        CRC32 crc = new CRC32();
        crc.update(data, 0, data.length);
        return crc.getValue();
    }

    /**
     * Computes the CRC32 checksum over a slice of the provided byte array.
     */
    public static long compute(byte[] data, int offset, int length) {
        if (data == null || length <= 0) {
            return 0L;
        }
        CRC32 crc = new CRC32();
        crc.update(data, offset, length);
        return crc.getValue();
    }

    /**
     * Computes the CRC32 checksum for an entire ByteBuffer slice from position to limit.
     */
    public static long compute(ByteBuffer buffer) {
        if (buffer == null || !buffer.hasRemaining()) {
            return 0L;
        }
        CRC32 crc = new CRC32();
        int originalPosition = buffer.position();
        crc.update(buffer);
        buffer.position(originalPosition);
        return crc.getValue();
    }

    /**
     * Validates whether expected checksum equals actual checksum.
     */
    public static boolean verify(long expectedChecksum, long actualChecksum) {
        return expectedChecksum == actualChecksum;
    }
}
