package se.mouaz.aegisdb.storage.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.storage.wal.CorruptedWalException;
import se.mouaz.aegisdb.storage.wal.WalChecksum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Robust, atomic snapshot writer implementation conforming to Master Project Plan §8.
 * Format:
 * - Magic Number: 0xAE615DA2 (4 bytes)
 * - Version: 1 (1 byte)
 * - LastIncludedIndex: int64 (8 bytes)
 * - LastIncludedTerm: int64 (8 bytes)
 * - Checksum: CRC32 of payload (4 bytes)
 * - PayloadLength: int32 (4 bytes)
 * - PayloadData: byte[PayloadLength]
 */
public class FileSnapshotWriter implements SnapshotWriter {
    private static final Logger log = LoggerFactory.getLogger(FileSnapshotWriter.class);

    public static final int SNAPSHOT_MAGIC = 0xAE615DA2;
    public static final byte SNAPSHOT_VERSION = 1;
    public static final int HEADER_SIZE = 4 + 1 + 8 + 8 + 4 + 4; // 29 bytes
    public static final int MAX_SNAPSHOT_PAYLOAD_SIZE = 16 * 1024 * 1024; // 16 MB limit
    private static final Pattern SNAPSHOT_FILE_PATTERN = Pattern.compile("^snapshot-(\\d{20})-(\\d{20})\\.snap$");

    private final Path snapshotDir;
    private final int maxRetainedSnapshots;

    public FileSnapshotWriter(Path snapshotDir, int maxRetainedSnapshots) throws IOException {
        this.snapshotDir = Objects.requireNonNull(snapshotDir, "snapshotDir cannot be null").toAbsolutePath().normalize();
        this.maxRetainedSnapshots = Math.max(1, maxRetainedSnapshots);
        Files.createDirectories(this.snapshotDir);
    }

    public FileSnapshotWriter(Path snapshotDir) throws IOException {
        this(snapshotDir, 2);
    }

    @Override
    public SnapshotWriteResult writeSnapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] stateMachineData) throws IOException {
        if (stateMachineData != null && stateMachineData.length > MAX_SNAPSHOT_PAYLOAD_SIZE) {
            throw new IllegalArgumentException("Snapshot data size " + stateMachineData.length +
                    " exceeds maximum permitted limit of " + MAX_SNAPSHOT_PAYLOAD_SIZE + " bytes");
        }

        byte[] payload = stateMachineData == null ? new byte[0] : stateMachineData;
        int payloadLen = payload.length;
        long checksum = WalChecksum.compute(payload, 0, payloadLen);

        String finalFileName = String.format("snapshot-%020d-%020d.snap", lastIncludedIndex, lastIncludedTerm);
        Path finalPath = snapshotDir.resolve(finalFileName).normalize();
        if (!finalPath.startsWith(snapshotDir)) {
            throw new SecurityException("Path traversal attempt in snapshot filename: " + finalFileName);
        }

        String tmpFileName = finalFileName + ".tmp";
        Path tmpPath = snapshotDir.resolve(tmpFileName).normalize();

        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + payloadLen);
        buffer.putInt(SNAPSHOT_MAGIC);
        buffer.put(SNAPSHOT_VERSION);
        buffer.putLong(lastIncludedIndex);
        buffer.putLong(lastIncludedTerm);
        buffer.putInt((int) checksum);
        buffer.putInt(payloadLen);
        buffer.put(payload);
        buffer.flip();

        try (FileChannel channel = FileChannel.open(tmpPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }

        Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        log.info("Persisted atomic snapshot to {}: lastIncludedIndex={}, lastIncludedTerm={}, size={} bytes",
                finalFileName, lastIncludedIndex, lastIncludedTerm, HEADER_SIZE + payloadLen);

        purgeOldSnapshots();
        return new SnapshotWriteResult(lastIncludedIndex, lastIncludedTerm, finalPath);
    }

    private void purgeOldSnapshots() {
        try (Stream<Path> stream = Files.list(snapshotDir)) {
            List<Path> snapshots = stream
                    .filter(p -> SNAPSHOT_FILE_PATTERN.matcher(p.getFileName().toString()).matches())
                    .sorted(Comparator.comparing(this::extractIndexFromFileName).reversed())
                    .toList();

            if (snapshots.size() > maxRetainedSnapshots) {
                for (int i = maxRetainedSnapshots; i < snapshots.size(); i++) {
                    Path oldSnapshot = snapshots.get(i);
                    try {
                        Files.deleteIfExists(oldSnapshot);
                        log.debug("Purged old snapshot: {}", oldSnapshot.getFileName());
                    } catch (IOException e) {
                        log.warn("Failed to purge old snapshot {}: {}", oldSnapshot, e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            log.warn("Failed to list snapshots for retention purge: {}", e.getMessage());
        }
    }

    private long extractIndexFromFileName(Path path) {
        Matcher matcher = SNAPSHOT_FILE_PATTERN.matcher(path.getFileName().toString());
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        return -1L;
    }

    @Override
    public void close() {
        // No persistent resources to hold open
    }
}
