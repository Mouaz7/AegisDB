package se.mouaz.aegisdb.transaction.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/**
 * Crash-durable, append-only disk coordinator journal for Two-Phase Commit state transitions (Master Project Plan §10; US015).
 * Features CRC32 checksum framing, magic header validation, and torn-write truncation.
 */
public class DurableCoordinatorLog implements TransactionCoordinatorLog {
    private static final Logger log = LoggerFactory.getLogger(DurableCoordinatorLog.class);

    public static final int MAGIC_HEADER = 0xAE6120C0; // AegisDB 2PC Coordinator Log Magic
    public static final byte FORMAT_VERSION = 1;

    private final Path logFilePath;
    private final FileChannel channel;
    private final boolean syncOnWrite;
    private final ReentrantLock appendLock = new ReentrantLock();
    private final Map<TransactionId, CoordinatorLogEntry> cachedState = new ConcurrentHashMap<>();

    public DurableCoordinatorLog(Path logFilePath) throws IOException {
        this(logFilePath, true);
    }

    public DurableCoordinatorLog(Path logFilePath, boolean syncOnWrite) throws IOException {
        this.logFilePath = Objects.requireNonNull(logFilePath, "logFilePath cannot be null");
        this.syncOnWrite = syncOnWrite;

        if (logFilePath.getParent() != null) {
            Files.createDirectories(logFilePath.getParent());
        }

        this.channel = FileChannel.open(
                logFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        scanAndRecover();
    }

    @Override
    public void logState(TransactionId txId, TwoPhaseCommitState state, Set<ShardId> participants) {
        Objects.requireNonNull(txId, "txId cannot be null");
        Objects.requireNonNull(state, "state cannot be null");

        long timestamp = System.currentTimeMillis();
        CoordinatorLogEntry entry = new CoordinatorLogEntry(txId, state, participants, timestamp);

        appendLock.lock();
        try {
            byte[] payload = encodePayload(txId, state, participants, timestamp);

            CRC32 crc = new CRC32();
            crc.update(payload);
            long checksum = crc.getValue();

            int recordLength = 4 + 1 + 4 + 8 + payload.length; // magic + version + length + crc + payload
            ByteBuffer buf = ByteBuffer.allocate(recordLength);
            buf.putInt(MAGIC_HEADER);
            buf.put(FORMAT_VERSION);
            buf.putInt(payload.length);
            buf.putLong(checksum);
            buf.put(payload);
            buf.flip();

            while (buf.hasRemaining()) {
                channel.write(buf);
            }

            if (syncOnWrite || state.isDecided()) {
                channel.force(false);
            }

            cachedState.put(txId, entry);
        } catch (IOException e) {
            throw new RuntimeException("Failed to append coordinator log entry for tx " + txId, e);
        } finally {
            appendLock.unlock();
        }
    }

    @Override
    public Map<TransactionId, CoordinatorLogEntry> recover() {
        return Collections.unmodifiableMap(new HashMap<>(cachedState));
    }

    @Override
    public Optional<CoordinatorLogEntry> getEntry(TransactionId txId) {
        return Optional.ofNullable(cachedState.get(txId));
    }

    private void scanAndRecover() throws IOException {
        channel.position(0);
        if (channel.size() > 0) {
            if (channel.size() < 4) {
                throw new IOException("Invalid log file: size less than magic header length");
            }
            ByteBuffer magicBuf = ByteBuffer.allocate(4);
            channel.position(0);
            channel.read(magicBuf);
            magicBuf.flip();
            int magic = magicBuf.getInt();
            if (magic != MAGIC_HEADER) {
                throw new IOException("Invalid magic header: 0x" + Integer.toHexString(magic));
            }
        }

        long validPosition = 0;

        while (validPosition + 17 <= channel.size()) {
            ByteBuffer headerBuf = ByteBuffer.allocate(17);
            channel.position(validPosition);
            int read = channel.read(headerBuf);
            if (read < 17) {
                break;
            }
            headerBuf.flip();

            int magic = headerBuf.getInt();
            byte version = headerBuf.get();
            int payloadLen = headerBuf.getInt();
            long expectedChecksum = headerBuf.getLong();

            if (version != FORMAT_VERSION) {
                if (validPosition == 0) {
                    throw new IOException("Unsupported log format version: " + version);
                }
                log.warn("Corrupt coordinator record version at position {}. Truncating.", validPosition);
                break;
            }

            if (payloadLen < 0) {
                log.warn("Corrupt coordinator record payload length at position {}. Truncating.", validPosition);
                break;
            }

            if (validPosition + 17 + payloadLen > channel.size()) {
                log.warn("Torn coordinator write detected at position {}. Truncating tail.", validPosition);
                break;
            }

            ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen);
            channel.read(payloadBuf);
            payloadBuf.flip();
            byte[] payload = payloadBuf.array();

            CRC32 crc = new CRC32();
            crc.update(payload);
            if (crc.getValue() != expectedChecksum) {
                log.warn("CRC32 mismatch at position {}. Expected {}, got {}. Truncating.",
                        validPosition, expectedChecksum, crc.getValue());
                break;
            }

            // Valid record, decode and update state
            CoordinatorLogEntry entry = decodePayload(payload);
            cachedState.put(entry.txId(), entry);

            validPosition += 17 + payloadLen;
        }

        if (validPosition < channel.size()) {
            log.info("Truncating coordinator log file to last valid position {}", validPosition);
            channel.truncate(validPosition);
        }
        channel.position(validPosition);
    }

    private static byte[] encodePayload(TransactionId txId, TwoPhaseCommitState state,
                                        Set<ShardId> participants, long timestamp) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeLong(txId.value());
            dos.writeByte(state.ordinal());
            dos.writeLong(timestamp);
            dos.writeInt(participants != null ? participants.size() : 0);
            if (participants != null) {
                for (ShardId s : participants) {
                    dos.writeUTF(s.value());
                }
            }
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Serialization error for coordinator payload", e);
        }
    }

    private static CoordinatorLogEntry decodePayload(byte[] payload) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(payload);
             DataInputStream dis = new DataInputStream(bais)) {
            long txVal = dis.readLong();
            byte stateOrd = dis.readByte();
            long timestamp = dis.readLong();
            int partCount = dis.readInt();
            Set<ShardId> participants = new LinkedHashSet<>();
            for (int i = 0; i < partCount; i++) {
                participants.add(ShardId.of(dis.readUTF()));
            }

            TwoPhaseCommitState state = TwoPhaseCommitState.values()[stateOrd];
            return new CoordinatorLogEntry(TransactionId.of(txVal), state, participants, timestamp);
        } catch (IOException e) {
            throw new RuntimeException("Deserialization error for coordinator payload", e);
        }
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
            log.warn("Failed to close coordinator log channel", e);
        } finally {
            appendLock.unlock();
        }
    }
}
