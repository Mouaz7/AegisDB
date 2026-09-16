package se.mouaz.aegisdb.storage.wal;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests verifying invariants for StorageRecord serialization,
 * deserialization, and checksum validation across arbitrary generated data.
 */
class StorageRecordPropertyTest {

    @Provide
    Arbitrary<StorageRecord> records() {
        Arbitrary<Long> seqNos = Arbitraries.longs().greaterOrEqual(0);
        Arbitrary<Long> terms = Arbitraries.longs().greaterOrEqual(0);
        Arbitrary<Long> timestamps = Arbitraries.longs().greaterOrEqual(0);
        Arbitrary<byte[]> keys = Arbitraries.bytes().array(byte[].class).ofMinSize(0).ofMaxSize(1024);
        Arbitrary<byte[]> values = Arbitraries.bytes().array(byte[].class).ofMinSize(0).ofMaxSize(4096);

        return Combinators.combine(seqNos, terms, timestamps, keys, values)
                .as(StorageRecord::createEntry);
    }

    @Provide
    Arbitrary<StorageRecord> recordsWithPayload() {
        Arbitrary<Long> seqNos = Arbitraries.longs().greaterOrEqual(0);
        Arbitrary<Long> terms = Arbitraries.longs().greaterOrEqual(0);
        Arbitrary<Long> timestamps = Arbitraries.longs().greaterOrEqual(0);
        Arbitrary<byte[]> keys = Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(256);
        Arbitrary<byte[]> values = Arbitraries.bytes().array(byte[].class).ofMinSize(1).ofMaxSize(1024);

        return Combinators.combine(seqNos, terms, timestamps, keys, values)
                .as(StorageRecord::createEntry);
    }

    @Property
    void roundtripSerializationPreservesFields(@ForAll("records") StorageRecord original) {
        ByteBuffer buf = original.serialize();
        assertThat(buf.remaining()).isEqualTo(original.totalSizeOnDisk());

        StorageRecord deserialized = StorageRecord.deserialize(buf);

        assertThat(deserialized.magicNumber()).isEqualTo(original.magicNumber());
        assertThat(deserialized.version()).isEqualTo(original.version());
        assertThat(deserialized.recordType()).isEqualTo(original.recordType());
        assertThat(deserialized.sequenceNumber()).isEqualTo(original.sequenceNumber());
        assertThat(deserialized.term()).isEqualTo(original.term());
        assertThat(deserialized.timestamp()).isEqualTo(original.timestamp());
        assertThat(deserialized.key()).isEqualTo(original.key());
        assertThat(deserialized.value()).isEqualTo(original.value());
        assertThat(deserialized.checksum()).isEqualTo(original.checksum());
        assertThat(deserialized.isValidChecksum()).isTrue();
        assertThat(deserialized).isEqualTo(original);
    }

    @Property
    void checksumDetectsSingleBytePayloadCorruption(@ForAll("recordsWithPayload") StorageRecord original) {
        ByteBuffer buf = original.serialize();
        byte[] rawBytes = buf.array();

        // Mutate the last byte in the value payload area (guaranteed to be within payload, avoiding length headers)
        int payloadOffset = rawBytes.length - 1;
        rawBytes[payloadOffset] ^= 0x5A; // Flip bits

        ByteBuffer corruptedBuf = ByteBuffer.wrap(rawBytes);
        StorageRecord corrupted = StorageRecord.deserialize(corruptedBuf);

        assertThat(corrupted.isValidChecksum())
                .as("Checksum validation must fail when payload bytes are corrupted")
                .isFalse();
    }
}
