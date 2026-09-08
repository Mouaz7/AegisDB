package se.mouaz.aegisdb.storage.wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class StorageRecordTest {

    @Test
    @DisplayName("StorageRecord serialization roundtrip preserves all fields")
    void testSerializationRoundtrip() {
        byte[] key = "user:42".getBytes(StandardCharsets.UTF_8);
        byte[] value = "{\"name\":\"Alice\",\"balance\":1000}".getBytes(StandardCharsets.UTF_8);
        long now = System.currentTimeMillis();

        StorageRecord record = StorageRecord.createEntry(1L, 2L, now, key, value);

        assertThat(record.magicNumber()).isEqualTo(StorageRecord.MAGIC_NUMBER);
        assertThat(record.version()).isEqualTo(StorageRecord.CURRENT_VERSION);
        assertThat(record.recordType()).isEqualTo(StorageRecord.TYPE_DATA);
        assertThat(record.sequenceNumber()).isEqualTo(1L);
        assertThat(record.term()).isEqualTo(2L);
        assertThat(record.timestamp()).isEqualTo(now);
        assertThat(record.key()).isEqualTo(key);
        assertThat(record.value()).isEqualTo(value);
        assertThat(record.isValidChecksum()).isTrue();

        ByteBuffer serialized = record.serialize();
        assertThat(serialized.remaining()).isEqualTo(record.totalSizeOnDisk());
    }

    @Test
    @DisplayName("StorageRecord detects checksum mismatch on payload modification")
    void testChecksumValidation() {
        byte[] key = "account:1".getBytes(StandardCharsets.UTF_8);
        byte[] val = "500".getBytes(StandardCharsets.UTF_8);
        StorageRecord record = StorageRecord.createEntry(10L, 3L, 1000L, key, val);

        assertThat(record.isValidChecksum()).isTrue();

        // Alter key in place
        record.key()[0] = 'X';
        assertThat(record.isValidChecksum()).isFalse();
    }

    @Test
    @DisplayName("StorageRecord handles empty keys and values")
    void testEmptyKeyAndValue() {
        StorageRecord record = StorageRecord.createEntry(5L, 1L, 2000L, new byte[0]);
        assertThat(record.key()).isEmpty();
        assertThat(record.value()).isEmpty();
        assertThat(record.isValidChecksum()).isTrue();
    }
}
