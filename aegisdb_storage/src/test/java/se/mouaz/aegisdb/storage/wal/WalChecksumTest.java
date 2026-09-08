package se.mouaz.aegisdb.storage.wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class WalChecksumTest {

    @Test
    @DisplayName("WalChecksum calculates consistent CRC32 values")
    void testChecksumConsistency() {
        byte[] data1 = "Hello AegisDB WAL".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "Hello AegisDB WAL".getBytes(StandardCharsets.UTF_8);

        long crc1 = WalChecksum.compute(data1);
        long crc2 = WalChecksum.compute(data2);

        assertThat(crc1).isNotZero();
        assertThat(crc1).isEqualTo(crc2);
        assertThat(WalChecksum.verify(crc1, crc2)).isTrue();
    }

    @Test
    @DisplayName("WalChecksum detects bit flip modifications")
    void testBitFlipDetection() {
        byte[] data = "Transaction Payload 12345".getBytes(StandardCharsets.UTF_8);
        long originalCrc = WalChecksum.compute(data);

        // Mutate one bit
        data[5] ^= 0x01;
        long corruptedCrc = WalChecksum.compute(data);

        assertThat(corruptedCrc).isNotEqualTo(originalCrc);
        assertThat(WalChecksum.verify(originalCrc, corruptedCrc)).isFalse();
    }

    @Test
    @DisplayName("WalChecksum handles empty or null data safely")
    void testEmptyAndNullData() {
        assertThat(WalChecksum.compute((byte[]) null)).isZero();
        assertThat(WalChecksum.compute(new byte[0])).isZero();
        assertThat(WalChecksum.compute((ByteBuffer) null)).isZero();
    }
}
