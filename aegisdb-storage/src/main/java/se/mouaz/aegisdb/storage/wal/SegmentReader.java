package se.mouaz.aegisdb.storage.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Optional;

/**
 * Sequential reader for a single WAL segment file (Master Project Plan §8, §17).
 * Handles clean EOF, detects torn tail records at EOF for safe recovery,
 * and detects data corruption via CRC32 verification.
 */
public class SegmentReader implements Closeable {

    public sealed interface ReadResult permits ReadResult.Success, ReadResult.EndOfFile, ReadResult.TornTail {
        record Success(StorageRecord record, long fileOffset, int recordSize) implements ReadResult {}
        record EndOfFile(long fileOffset) implements ReadResult {}
        record TornTail(long fileOffset, String reason) implements ReadResult {}
    }

    private final WalSegment segment;
    private final FileChannel channel;
    private final ByteBuffer headerBuffer = ByteBuffer.allocate(StorageRecord.FRAMING_HEADER_SIZE);

    public SegmentReader(WalSegment segment) throws IOException {
        this.segment = Objects.requireNonNull(segment, "segment cannot be null");
        this.channel = segment.openChannel(StandardOpenOption.READ);
    }

    public WalSegment segment() {
        return segment;
    }

    public long position() throws IOException {
        return channel.position();
    }

    public void seek(long offset) throws IOException {
        channel.position(offset);
    }

    /**
     * Reads the next record in the segment.
     *
     * @return ReadResult indicating Success with record, EndOfFile, or TornTail at EOF.
     * @throws CorruptedWalException if magic number or CRC32 checksum fails on an intact record.
     */
    public ReadResult readNext() throws IOException {
        long recordStartOffset = channel.position();
        long fileSize = channel.size();

        if (recordStartOffset >= fileSize) {
            return new ReadResult.EndOfFile(recordStartOffset);
        }

        // 1. Read Framing Header (11 bytes: magic 4, version 2, type 1, length 4)
        headerBuffer.clear();
        int bytesRead = channel.read(headerBuffer);
        if (bytesRead <= 0) {
            return new ReadResult.EndOfFile(recordStartOffset);
        }
        if (bytesRead < StorageRecord.FRAMING_HEADER_SIZE) {
            // Incomplete header at EOF -> torn write
            return new ReadResult.TornTail(recordStartOffset, "Incomplete framing header: read " + bytesRead + " bytes");
        }

        headerBuffer.flip();
        int magic = headerBuffer.getInt();
        short version = headerBuffer.getShort();
        byte recordType = headerBuffer.get();
        int recordLength = headerBuffer.getInt();

        // Validate Magic Number
        if (magic != StorageRecord.MAGIC_NUMBER) {
            throw new CorruptedWalException(String.format(
                    "Invalid WAL magic number 0x%08X at offset %d in segment %s (expected 0x%08X)",
                    magic, recordStartOffset, segment.path().getFileName(), StorageRecord.MAGIC_NUMBER
            ));
        }

        // Validate Version
        if (version != StorageRecord.CURRENT_VERSION) {
            throw new CorruptedWalException(String.format(
                    "Unsupported WAL format version %d at offset %d in segment %s (expected %d)",
                    version, recordStartOffset, segment.path().getFileName(), StorageRecord.CURRENT_VERSION
            ));
        }

        // Validate Bounded Length (Master Project Plan §12)
        if (recordLength < StorageRecord.MIN_RECORD_LENGTH || recordLength > StorageRecord.MAX_RECORD_SIZE) {
            throw new CorruptedWalException(String.format(
                    "WAL record length %d at offset %d exceeds safety bounds [min=%d, max=%d] in segment %s",
                    recordLength, recordStartOffset, StorageRecord.MIN_RECORD_LENGTH,
                    StorageRecord.MAX_RECORD_SIZE, segment.path().getFileName()
            ));
        }

        // 2. Read Remaining Record Payload
        if (recordStartOffset + StorageRecord.FRAMING_HEADER_SIZE + recordLength > fileSize) {
            // Tail truncated mid-record -> torn write at EOF
            return new ReadResult.TornTail(recordStartOffset, String.format(
                    "Incomplete payload at EOF: expected %d bytes, but file ends at %d",
                    recordLength, fileSize
            ));
        }

        ByteBuffer payloadBuffer = ByteBuffer.allocate(recordLength);
        while (payloadBuffer.hasRemaining()) {
            int read = channel.read(payloadBuffer);
            if (read == -1) {
                return new ReadResult.TornTail(recordStartOffset, "Unexpected EOF while reading record payload");
            }
        }
        payloadBuffer.flip();

        // 3. Parse Checksum (4 bytes)
        long storedChecksum = payloadBuffer.getInt() & 0xFFFFFFFFL;

        // 4. Parse Body (32 bytes base)
        long sequenceNumber = payloadBuffer.getLong();
        long timestamp = payloadBuffer.getLong();
        long term = payloadBuffer.getLong();

        int keyLength = payloadBuffer.getInt();
        if (keyLength < 0 || keyLength > payloadBuffer.remaining()) {
            throw new CorruptedWalException("Invalid keyLength " + keyLength + " at offset " + recordStartOffset);
        }
        byte[] key = new byte[keyLength];
        payloadBuffer.get(key);

        int valueLength = payloadBuffer.getInt();
        if (valueLength < 0 || valueLength > payloadBuffer.remaining()) {
            throw new CorruptedWalException("Invalid valueLength " + valueLength + " at offset " + recordStartOffset);
        }
        byte[] value = new byte[valueLength];
        payloadBuffer.get(value);

        // 5. Verify Checksum
        long calculatedChecksum = StorageRecord.computeChecksum(sequenceNumber, timestamp, term, recordType, key, value);
        if (!WalChecksum.verify(storedChecksum, calculatedChecksum)) {
            throw new CorruptedWalException(String.format(
                    "CRC32 Checksum mismatch at offset %d in segment %s (expected=0x%08X, calculated=0x%08X)",
                    recordStartOffset, segment.path().getFileName(), storedChecksum, calculatedChecksum
            ));
        }

        StorageRecord record = new StorageRecord(
                magic, version, recordType, storedChecksum, sequenceNumber, timestamp, term, key, value
        );

        int totalRecordSize = StorageRecord.FRAMING_HEADER_SIZE + recordLength;
        return new ReadResult.Success(record, recordStartOffset, totalRecordSize);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
