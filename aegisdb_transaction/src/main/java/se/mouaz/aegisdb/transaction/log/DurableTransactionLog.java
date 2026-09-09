package se.mouaz.aegisdb.transaction.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.transaction.OperationType;
import se.mouaz.aegisdb.transaction.TransactionException;
import se.mouaz.aegisdb.transaction.TransactionState;
import se.mouaz.aegisdb.transaction.WriteOperation;
import se.mouaz.aegisdb.transaction.WriteSet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * High-performance append-only durable transaction log with CRC32 framing and crash recovery
 * (Master Project Plan §8, §9 & §18).
 */
public class DurableTransactionLog implements TransactionLog {
    private static final Logger log = LoggerFactory.getLogger(DurableTransactionLog.class);

    public static final int MAGIC_HEADER = 0xAE615D70; // AegisDB Tx Log Magic
    public static final byte FORMAT_VERSION = 1;

    private final Path logFilePath;
    private final FileChannel channel;
    private final boolean syncOnCommit;
    private final ReentrantLock appendLock = new ReentrantLock();
    private final AtomicLong sequenceGenerator = new AtomicLong(0);

    public DurableTransactionLog(Path logFilePath) throws IOException {
        this(logFilePath, true);
    }

    public DurableTransactionLog(Path logFilePath, boolean syncOnCommit) throws IOException {
        this.logFilePath = Objects.requireNonNull(logFilePath, "logFilePath must not be null");
        this.syncOnCommit = syncOnCommit;

        if (logFilePath.getParent() != null) {
            Files.createDirectories(logFilePath.getParent());
        }

        this.channel = FileChannel.open(
                logFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        // Scan to find last sequence number and validate integrity
        long lastSeq = scanAndRecoverTail();
        this.sequenceGenerator.set(lastSeq);
    }

    @Override
    public void logBegin(TransactionId txId, long timestamp) {
        long seq = sequenceGenerator.incrementAndGet();
        appendRecord(seq, txId, TransactionState.ACTIVE, timestamp, Collections.emptyList(), false);
    }

    @Override
    public void logPrepare(TransactionId txId, long timestamp, WriteSet writeSet) {
        long seq = sequenceGenerator.incrementAndGet();
        List<WriteOperation> writes = (writeSet != null) ? new ArrayList<>(writeSet.operations().values()) : Collections.emptyList();
        appendRecord(seq, txId, TransactionState.PREPARING, timestamp, writes, syncOnCommit);
    }

    @Override
    public void logCommit(TransactionId txId, long commitTimestamp, WriteSet writeSet) {
        long seq = sequenceGenerator.incrementAndGet();
        List<WriteOperation> writes = (writeSet != null) ? new ArrayList<>(writeSet.operations().values()) : Collections.emptyList();
        appendRecord(seq, txId, TransactionState.COMMITTED, commitTimestamp, writes, syncOnCommit);
    }

    @Override
    public void logAbort(TransactionId txId, long timestamp) {
        long seq = sequenceGenerator.incrementAndGet();
        appendRecord(seq, txId, TransactionState.ABORTED, timestamp, Collections.emptyList(), syncOnCommit);
    }

    private void appendRecord(long seq,
                              TransactionId txId,
                              TransactionState state,
                              long timestamp,
                              List<WriteOperation> writes,
                              boolean forceSync) {
        appendLock.lock();
        try {
            byte[] payload = encodePayload(seq, txId, state, timestamp, writes);

            CRC32 crc = new CRC32();
            crc.update(payload);
            long checksum = crc.getValue();

            // Framing: Magic(4) + Version(1) + RecordLength(4) + Checksum(8) + Payload
            ByteBuffer buffer = ByteBuffer.allocate(4 + 1 + 4 + 8 + payload.length);
            buffer.putInt(MAGIC_HEADER);
            buffer.put(FORMAT_VERSION);
            buffer.putInt(payload.length);
            buffer.putLong(checksum);
            buffer.put(payload);
            buffer.flip();

            channel.position(channel.size());
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            if (forceSync) {
                channel.force(false);
            }
        } catch (IOException e) {
            throw new TransactionException("Failed to append to durable transaction log", e);
        } finally {
            appendLock.unlock();
        }
    }

    private byte[] encodePayload(long seq,
                                 TransactionId txId,
                                 TransactionState state,
                                 long timestamp,
                                 List<WriteOperation> writes) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(seq);
            dos.writeLong(txId.value());
            dos.writeByte(state.ordinal());
            dos.writeLong(timestamp);
            dos.writeInt(writes.size());

            for (WriteOperation op : writes) {
                dos.writeByte(op.type().ordinal());
                byte[] keyBytes = op.key().getBytes(StandardCharsets.UTF_8);
                dos.writeShort(keyBytes.length);
                dos.write(keyBytes);
                dos.writeInt(op.value().length);
                dos.write(op.value());
            }
            dos.flush();
        }
        return baos.toByteArray();
    }

    @Override
    public List<TransactionLogEntry> replay() {
        appendLock.lock();
        try {
            List<TransactionLogEntry> entries = new ArrayList<>();
            channel.position(0);
            ByteBuffer headerBuf = ByteBuffer.allocate(4 + 1 + 4 + 8);

            while (channel.position() + headerBuf.capacity() <= channel.size()) {
                headerBuf.clear();
                channel.read(headerBuf);
                headerBuf.flip();

                int magic = headerBuf.getInt();
                if (magic != MAGIC_HEADER) {
                    break;
                }
                byte version = headerBuf.get();
                if (version != FORMAT_VERSION) {
                    break;
                }
                int payloadLength = headerBuf.getInt();
                long expectedChecksum = headerBuf.getLong();

                if (payloadLength < 0 || channel.position() + payloadLength > channel.size()) {
                    break;
                }

                ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLength);
                channel.read(payloadBuf);
                payloadBuf.flip();

                byte[] payload = payloadBuf.array();
                CRC32 crc = new CRC32();
                crc.update(payload);
                if (crc.getValue() != expectedChecksum) {
                    log.warn("Corrupted record detected at position {}, stopping replay", channel.position() - payloadLength);
                    break;
                }

                TransactionLogEntry entry = decodePayload(payload);
                entries.add(entry);
            }
            return Collections.unmodifiableList(entries);
        } catch (IOException e) {
            throw new TransactionException("Failed to replay transaction log", e);
        } finally {
            appendLock.unlock();
        }
    }

    private TransactionLogEntry decodePayload(byte[] payload) throws IOException {
        try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(payload))) {
            long seq = dis.readLong();
            long txIdVal = dis.readLong();
            byte stateOrd = dis.readByte();
            TransactionState state = TransactionState.values()[stateOrd];
            long timestamp = dis.readLong();
            int opCount = dis.readInt();

            List<WriteOperation> writes = new ArrayList<>(opCount);
            for (int i = 0; i < opCount; i++) {
                byte typeOrd = dis.readByte();
                OperationType type = OperationType.values()[typeOrd];
                short keyLen = dis.readShort();
                byte[] keyBytes = new byte[keyLen];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int valLen = dis.readInt();
                byte[] valBytes = new byte[valLen];
                dis.readFully(valBytes);

                writes.add(new WriteOperation(key, type, valBytes));
            }
            return new TransactionLogEntry(seq, TransactionId.of(txIdVal), state, timestamp, writes);
        }
    }

    private long scanAndRecoverTail() throws IOException {
        channel.position(0);
        long lastValidPosition = 0;
        long lastSeq = 0;
        ByteBuffer headerBuf = ByteBuffer.allocate(4 + 1 + 4 + 8);

        while (channel.position() + headerBuf.capacity() <= channel.size()) {
            long recordStart = channel.position();
            headerBuf.clear();
            int read = channel.read(headerBuf);
            if (read < headerBuf.capacity()) {
                channel.truncate(recordStart);
                break;
            }
            headerBuf.flip();

            int magic = headerBuf.getInt();
            if (magic != MAGIC_HEADER) {
                log.warn("Invalid magic 0x{} at position {}, truncating torn tail", Integer.toHexString(magic), recordStart);
                channel.truncate(recordStart);
                break;
            }
            byte version = headerBuf.get();
            if (version != FORMAT_VERSION) {
                log.warn("Invalid format version {} at position {}, truncating", version, recordStart);
                channel.truncate(recordStart);
                break;
            }
            int payloadLength = headerBuf.getInt();
            long expectedCrc = headerBuf.getLong();

            if (payloadLength < 0 || channel.position() + payloadLength > channel.size()) {
                log.warn("Torn record length {} at position {}, truncating", payloadLength, recordStart);
                channel.truncate(recordStart);
                break;
            }

            ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLength);
            channel.read(payloadBuf);
            payloadBuf.flip();

            byte[] payload = payloadBuf.array();
            CRC32 crc = new CRC32();
            crc.update(payload);
            if (crc.getValue() != expectedCrc) {
                log.warn("CRC mismatch at position {}, truncating torn record", recordStart);
                channel.truncate(recordStart);
                break;
            }

            lastValidPosition = channel.position();
            try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(payload))) {
                lastSeq = dis.readLong();
            }
        }

        channel.position(channel.size());
        return lastSeq;
    }

    @Override
    public long lastSequenceNumber() {
        return sequenceGenerator.get();
    }

    public Path logFilePath() {
        return logFilePath;
    }

    @Override
    public void close() {
        appendLock.lock();
        try {
            if (channel.isOpen()) {
                channel.force(true);
                channel.close();
            }
        } catch (IOException e) {
            log.error("Failed to close durable transaction log channel", e);
        } finally {
            appendLock.unlock();
        }
    }
}
