package se.mouaz.aegisdb.storage.wal;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * StorageRecord represents a single framed record in the Write-Ahead Log (Master Project Plan §8).
 *
 * Suggested WAL record format:
 * - Magic number (4 bytes): 0xAE615DA1 (Detect invalid/corrupt format)
 * - Format version (2 bytes): 1 (Allow future format evolution)
 * - Record type (1 byte): DATA, METADATA, TRUNCATE, CHECKPOINT
 * - Record length (4 bytes): Safe record framing
 * - Checksum (4 bytes): CRC32 (Detect corruption/partial writes)
 * - Sequence number (8 bytes): Ordering / Raft LogIndex
 * - Timestamp (8 bytes): Diagnostics / versioning support
 * - Term (8 bytes): Raft term for consensus entries
 * - Key length (4 bytes) & Key (variable)
 * - Value length (4 bytes) & Value (variable)
 */
public record StorageRecord(
        int magicNumber,
        short version,
        byte recordType,
        long checksum,
        long sequenceNumber,
        long timestamp,
        long term,
        byte[] key,
        byte[] value
) {
    public static final int MAGIC_NUMBER = 0xAE615DA1;
    public static final short CURRENT_VERSION = 1;

    // Record types
    public static final byte TYPE_DATA = 1;
    public static final byte TYPE_METADATA = 2;
    public static final byte TYPE_TRUNCATE = 3;
    public static final byte TYPE_CHECKPOINT = 4;

    // Header sizes
    // Magic(4) + Version(2) + Type(1) + RecordLength(4) = 11 bytes framing header
    public static final int FRAMING_HEADER_SIZE = 11;
    // Checksum(4) = 4 bytes
    public static final int CHECKSUM_SIZE = 4;
    // Base body: SeqNo(8) + Timestamp(8) + Term(8) + KeyLen(4) + ValLen(4) = 32 bytes
    public static final int BASE_BODY_SIZE = 32;
    public static final int MIN_RECORD_LENGTH = CHECKSUM_SIZE + BASE_BODY_SIZE; // 36 bytes

    // Security bounds (Master Project Plan §12)
    public static final int MAX_RECORD_SIZE = 16 * 1024 * 1024; // 16 MB

    public StorageRecord {
        Objects.requireNonNull(key, "key cannot be null (use empty array if absent)");
        Objects.requireNonNull(value, "value cannot be null (use empty array if absent)");
        key = key.clone();
        value = value.clone();
    }

    @Override
    public byte[] key() {
        return key.clone();
    }

    @Override
    public byte[] value() {
        return value.clone();
    }

    /**
     * Factory method creating a DATA record for a Raft log entry.
     */
    public static StorageRecord createEntry(long sequenceNumber, long term, long timestamp, byte[] key, byte[] value) {
        byte[] safeKey = key != null ? key : new byte[0];
        byte[] safeVal = value != null ? value : new byte[0];
        long calculatedChecksum = computeChecksum(sequenceNumber, timestamp, term, TYPE_DATA, safeKey, safeVal);
        return new StorageRecord(
                MAGIC_NUMBER,
                CURRENT_VERSION,
                TYPE_DATA,
                calculatedChecksum,
                sequenceNumber,
                timestamp,
                term,
                safeKey,
                safeVal
        );
    }

    /**
     * Factory method creating a DATA record with empty key (pure payload).
     */
    public static StorageRecord createEntry(long sequenceNumber, long term, long timestamp, byte[] value) {
        return createEntry(sequenceNumber, term, timestamp, new byte[0], value);
    }

    /**
     * Factory method creating a TRUNCATE record (used when conflicting uncommitted log entries are discarded).
     */
    public static StorageRecord createTruncate(long truncateFromIndex, long term, long timestamp) {
        long calculatedChecksum = computeChecksum(truncateFromIndex, timestamp, term, TYPE_TRUNCATE, new byte[0], new byte[0]);
        return new StorageRecord(
                MAGIC_NUMBER,
                CURRENT_VERSION,
                TYPE_TRUNCATE,
                calculatedChecksum,
                truncateFromIndex,
                timestamp,
                term,
                new byte[0],
                new byte[0]
        );
    }

    /**
     * Returns the payload length following the recordLength field (checksum + body).
     */
    public int recordLength() {
        return CHECKSUM_SIZE + BASE_BODY_SIZE + key.length + value.length;
    }

    /**
     * Returns total size of the record on disk including framing header.
     */
    public int totalSizeOnDisk() {
        return FRAMING_HEADER_SIZE + recordLength();
    }

    /**
     * Encodes this record into a ByteBuffer ready for writing to disk.
     */
    public ByteBuffer serialize() {
        int totalSize = totalSizeOnDisk();
        ByteBuffer buf = ByteBuffer.allocate(totalSize);

        // 1. Framing Header (11 bytes)
        buf.putInt(magicNumber);
        buf.putShort(version);
        buf.put(recordType);
        buf.putInt(recordLength());

        // 2. Checksum (4 bytes, unsigned int stored in 4 bytes)
        buf.putInt((int) (checksum & 0xFFFFFFFFL));

        // 3. Body (32 bytes + payloads)
        buf.putLong(sequenceNumber);
        buf.putLong(timestamp);
        buf.putLong(term);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(value.length);
        buf.put(value);

        buf.flip();
        return buf;
    }

    /**
     * Deserializes a StorageRecord from a ByteBuffer. The buffer's position must be at the start of the record.
     */
    public static StorageRecord deserialize(ByteBuffer buf) {
        int magicNumber = buf.getInt();
        short version = buf.getShort();
        byte recordType = buf.get();
        int recordLength = buf.getInt();
        long checksum = Integer.toUnsignedLong(buf.getInt());
        long sequenceNumber = buf.getLong();
        long timestamp = buf.getLong();
        long term = buf.getLong();
        
        int keyLen = buf.getInt();
        byte[] key = new byte[keyLen];
        buf.get(key);
        
        int valLen = buf.getInt();
        byte[] value = new byte[valLen];
        buf.get(value);
        
        return new StorageRecord(
                magicNumber,
                version,
                recordType,
                checksum,
                sequenceNumber,
                timestamp,
                term,
                key,
                value
        );
    }

    /**
     * Computes the CRC32 checksum over the body fields.
     */
    public static long computeChecksum(long seqNo, long timestamp, long term, byte type, byte[] key, byte[] val) {
        ByteBuffer buf = ByteBuffer.allocate(BASE_BODY_SIZE + 1 + key.length + val.length);
        buf.put(type);
        buf.putLong(seqNo);
        buf.putLong(timestamp);
        buf.putLong(term);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(val.length);
        buf.put(val);
        buf.flip();
        return WalChecksum.compute(buf);
    }

    /**
     * Validates if the checksum stored in this record matches the actual computed checksum.
     */
    public boolean isValidChecksum() {
        long expected = computeChecksum(sequenceNumber, timestamp, term, recordType, key, value);
        return WalChecksum.verify(this.checksum, expected);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StorageRecord that = (StorageRecord) o;
        return magicNumber == that.magicNumber &&
                version == that.version &&
                recordType == that.recordType &&
                checksum == that.checksum &&
                sequenceNumber == that.sequenceNumber &&
                timestamp == that.timestamp &&
                term == that.term &&
                Arrays.equals(key, that.key) &&
                Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(magicNumber, version, recordType, checksum, sequenceNumber, timestamp, term);
        result = 31 * result + Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "StorageRecord{" +
                "magic=" + Integer.toHexString(magicNumber) +
                ", version=" + version +
                ", type=" + recordType +
                ", seqNo=" + sequenceNumber +
                ", term=" + term +
                ", timestamp=" + timestamp +
                ", keyLen=" + key.length +
                ", valLen=" + value.length +
                ", checksum=" + checksum +
                '}';
    }
}
