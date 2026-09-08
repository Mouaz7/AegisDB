package se.mouaz.aegisdb.storage.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.storage.wal.CorruptedWalException;
import se.mouaz.aegisdb.storage.wal.WalChecksum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static se.mouaz.aegisdb.storage.snapshot.FileSnapshotWriter.*;

/**
 * Robust snapshot reader implementation conforming to Master Project Plan §8.
 */
public class FileSnapshotReader implements SnapshotReader {
    private static final Logger log = LoggerFactory.getLogger(FileSnapshotReader.class);
    private static final Pattern SNAPSHOT_FILE_PATTERN = Pattern.compile("^snapshot-(\\d{20})-(\\d{20})\\.snap$");

    private final Path snapshotDir;

    public FileSnapshotReader(Path snapshotDir) throws IOException {
        this.snapshotDir = Objects.requireNonNull(snapshotDir, "snapshotDir cannot be null").toAbsolutePath().normalize();
        Files.createDirectories(this.snapshotDir);
    }

    @Override
    public Optional<SnapshotMetadata> loadLatestSnapshotMetadata() throws IOException {
        try (Stream<Path> stream = Files.list(snapshotDir)) {
            List<Path> snapshotFiles = stream
                    .filter(p -> SNAPSHOT_FILE_PATTERN.matcher(p.getFileName().toString()).matches())
                    .sorted(Comparator.comparing(this::extractIndexFromFileName).reversed())
                    .toList();

            for (Path path : snapshotFiles) {
                try {
                    SnapshotMetadata metadata = readMetadataAndValidate(path);
                    return Optional.of(metadata);
                } catch (CorruptedWalException ex) {
                    log.warn("Corrupted snapshot detected at {}: {}", path.getFileName(), ex.getMessage());
                    throw ex;
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public byte[] readSnapshotData(Path snapshotPath) throws IOException {
        Path path = snapshotPath.toAbsolutePath().normalize();
        if (!path.startsWith(snapshotDir)) {
            throw new SecurityException("Path traversal attempt in snapshot reading: " + snapshotPath);
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize < HEADER_SIZE) {
                throw new CorruptedWalException("Snapshot file " + path.getFileName() + " is too small: " + fileSize + " bytes");
            }

            ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
            while (headerBuf.hasRemaining()) {
                if (channel.read(headerBuf) < 0) {
                    throw new CorruptedWalException("Unexpected EOF while reading snapshot header in " + path.getFileName());
                }
            }
            headerBuf.flip();

            int magic = headerBuf.getInt();
            if (magic != SNAPSHOT_MAGIC) {
                throw new CorruptedWalException(String.format("Invalid snapshot magic in %s: expected 0x%08X but found 0x%08X",
                        path.getFileName(), SNAPSHOT_MAGIC, magic));
            }

            byte version = headerBuf.get();
            if (version != SNAPSHOT_VERSION) {
                throw new CorruptedWalException(String.format("Unsupported snapshot version in %s: %d", path.getFileName(), version));
            }

            long lastIncludedIndex = headerBuf.getLong();
            long lastIncludedTerm = headerBuf.getLong();
            long expectedChecksum = Integer.toUnsignedLong(headerBuf.getInt());
            int payloadLen = headerBuf.getInt();

            if (payloadLen < 0 || payloadLen > MAX_SNAPSHOT_PAYLOAD_SIZE) {
                throw new CorruptedWalException("Snapshot payload length out of bounds: " + payloadLen);
            }

            if (fileSize != HEADER_SIZE + payloadLen) {
                throw new CorruptedWalException(String.format("Snapshot file length mismatch in %s: expected %d, found %d",
                        path.getFileName(), HEADER_SIZE + payloadLen, fileSize));
            }

            ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen);
            while (payloadBuf.hasRemaining()) {
                if (channel.read(payloadBuf) < 0) {
                    throw new CorruptedWalException("Unexpected EOF while reading snapshot payload in " + path.getFileName());
                }
            }
            byte[] data = payloadBuf.array();

            long actualChecksum = WalChecksum.compute(data, 0, data.length);
            if (actualChecksum != expectedChecksum) {
                throw new CorruptedWalException(String.format("Snapshot checksum mismatch in %s: expected 0x%08X but computed 0x%08X",
                        path.getFileName(), expectedChecksum, actualChecksum));
            }

            return data;
        }
    }

    private SnapshotMetadata readMetadataAndValidate(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize < HEADER_SIZE) {
                throw new CorruptedWalException("Snapshot file " + path.getFileName() + " is too small: " + fileSize + " bytes");
            }

            ByteBuffer headerBuf = ByteBuffer.allocate(HEADER_SIZE);
            while (headerBuf.hasRemaining()) {
                if (channel.read(headerBuf) < 0) {
                    throw new CorruptedWalException("Unexpected EOF while reading snapshot header in " + path.getFileName());
                }
            }
            headerBuf.flip();

            int magic = headerBuf.getInt();
            if (magic != SNAPSHOT_MAGIC) {
                throw new CorruptedWalException(String.format("Invalid snapshot magic in %s: expected 0x%08X but found 0x%08X",
                        path.getFileName(), SNAPSHOT_MAGIC, magic));
            }

            byte version = headerBuf.get();
            if (version != SNAPSHOT_VERSION) {
                throw new CorruptedWalException(String.format("Unsupported snapshot version in %s: %d", path.getFileName(), version));
            }

            long lastIncludedIndex = headerBuf.getLong();
            long lastIncludedTerm = headerBuf.getLong();
            long expectedChecksum = Integer.toUnsignedLong(headerBuf.getInt());
            int payloadLen = headerBuf.getInt();

            if (payloadLen < 0 || payloadLen > MAX_SNAPSHOT_PAYLOAD_SIZE) {
                throw new CorruptedWalException("Snapshot payload length out of bounds: " + payloadLen);
            }

            if (fileSize != HEADER_SIZE + payloadLen) {
                throw new CorruptedWalException(String.format("Snapshot file length mismatch in %s: expected %d, found %d",
                        path.getFileName(), HEADER_SIZE + payloadLen, fileSize));
            }

            // Quick checksum verification of payload
            ByteBuffer payloadBuf = ByteBuffer.allocate(payloadLen);
            while (payloadBuf.hasRemaining()) {
                channel.read(payloadBuf);
            }
            long actualChecksum = WalChecksum.compute(payloadBuf.array(), 0, payloadLen);
            if (actualChecksum != expectedChecksum) {
                throw new CorruptedWalException(String.format("Snapshot checksum mismatch in %s: expected 0x%08X but computed 0x%08X",
                        path.getFileName(), expectedChecksum, actualChecksum));
            }

            return new SnapshotMetadata(lastIncludedIndex, lastIncludedTerm, expectedChecksum, path);
        }
    }

    private long extractIndexFromFileName(Path path) {
        Matcher matcher = SNAPSHOT_FILE_PATTERN.matcher(path.getFileName().toString());
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        return -1L;
    }

    public List<SnapshotMetadata> listSnapshots() throws IOException {
        try (Stream<Path> stream = Files.list(snapshotDir)) {
            return stream
                    .filter(p -> SNAPSHOT_FILE_PATTERN.matcher(p.getFileName().toString()).matches())
                    .sorted(Comparator.comparing(this::extractIndexFromFileName).reversed())
                    .map(p -> {
                        try {
                            return readMetadataAndValidate(p);
                        } catch (IOException e) {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .toList();
        }
    }

    @Override
    public void close() {
        // No persistent resources to hold open
    }
}
