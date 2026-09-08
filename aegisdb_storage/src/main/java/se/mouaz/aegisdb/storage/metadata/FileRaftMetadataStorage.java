package se.mouaz.aegisdb.storage.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.storage.wal.CorruptedWalException;
import se.mouaz.aegisdb.storage.wal.WalChecksum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Objects;
import java.util.Optional;

/**
 * File-based atomic and durable storage for Raft metadata (term and vote) (Master Project Plan §7, §8, §17).
 * Employs write-to-temp + fsync + atomic rename (ATOMIC_MOVE) with CRC32 verification.
 */
public class FileRaftMetadataStorage implements RaftMetadataStorage {
    private static final Logger log = LoggerFactory.getLogger(FileRaftMetadataStorage.class);

    public static final int METADATA_MAGIC = 0xAE615D11;
    public static final String METADATA_FILE_NAME = "raft-metadata.meta";
    public static final String METADATA_TMP_SUFFIX = ".tmp";

    private final Path metadataFile;
    private final Path tempFile;

    public FileRaftMetadataStorage(Path directory) throws IOException {
        Objects.requireNonNull(directory, "directory cannot be null");
        if (!Files.exists(directory)) {
            Files.createDirectories(directory);
        }
        this.metadataFile = directory.resolve(METADATA_FILE_NAME);
        this.tempFile = directory.resolve(METADATA_FILE_NAME + METADATA_TMP_SUFFIX);
    }

    public Path metadataFile() {
        return metadataFile;
    }

    @Override
    public synchronized void save(long term, NodeId votedFor) throws IOException {
        String votedForStr = votedFor != null ? votedFor.value() : "";
        byte[] votedForBytes = votedForStr.getBytes(StandardCharsets.UTF_8);

        // Framing:
        // Magic (4 bytes) + Term (8 bytes) + VotedForLength (4 bytes) + VotedFor (variable) + Checksum (4 bytes)
        int bodyLength = 4 + 8 + 4 + votedForBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(bodyLength + 4);

        buffer.putInt(METADATA_MAGIC);
        buffer.putLong(term);
        buffer.putInt(votedForBytes.length);
        buffer.put(votedForBytes);

        // Compute checksum over body (0 to bodyLength)
        long checksum = WalChecksum.compute(buffer.array(), 0, bodyLength);
        buffer.putInt((int) (checksum & 0xFFFFFFFFL));
        buffer.flip();

        // 1. Write to temporary file
        try (FileChannel channel = FileChannel.open(tempFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }

        // 2. Atomic rename to target metadata file
        try {
            Files.move(tempFile, metadataFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            // Fallback if filesystem does not support ATOMIC_MOVE
            Files.move(tempFile, metadataFile, StandardCopyOption.REPLACE_EXISTING);
        }

        log.debug("Persisted Raft metadata: term={}, votedFor={}", term, votedFor);
    }

    @Override
    public synchronized Optional<PersistentRaftMetadata> load() throws IOException {
        if (!Files.exists(metadataFile)) {
            return Optional.empty();
        }

        byte[] allBytes = Files.readAllBytes(metadataFile);
        if (allBytes.length < 16) {
            throw new CorruptedWalException("Raft metadata file is truncated: length " + allBytes.length);
        }

        ByteBuffer buffer = ByteBuffer.wrap(allBytes);
        int magic = buffer.getInt();
        if (magic != METADATA_MAGIC) {
            throw new CorruptedWalException(String.format("Invalid Raft metadata magic: 0x%08X", magic));
        }

        long term = buffer.getLong();
        int votedForLen = buffer.getInt();
        if (votedForLen < 0 || votedForLen > buffer.remaining() - 4) {
            throw new CorruptedWalException("Invalid votedFor length in metadata: " + votedForLen);
        }

        byte[] votedForBytes = new byte[votedForLen];
        buffer.get(votedForBytes);

        long storedChecksum = buffer.getInt() & 0xFFFFFFFFL;

        // Verify checksum over body
        ByteBuffer verifyBuf = ByteBuffer.wrap(allBytes, 0, allBytes.length - 4);
        long calculatedChecksum = WalChecksum.compute(verifyBuf);
        if (!WalChecksum.verify(storedChecksum, calculatedChecksum)) {
            throw new CorruptedWalException(String.format(
                    "CRC32 mismatch in Raft metadata file: stored=0x%08X, calculated=0x%08X",
                    storedChecksum, calculatedChecksum
            ));
        }

        NodeId votedFor = votedForLen > 0 ? NodeId.of(new String(votedForBytes, StandardCharsets.UTF_8)) : null;
        return Optional.of(new PersistentRaftMetadata(term, votedFor));
    }
}
