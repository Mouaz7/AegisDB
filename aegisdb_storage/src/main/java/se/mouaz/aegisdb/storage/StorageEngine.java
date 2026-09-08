package se.mouaz.aegisdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.storage.metadata.FileRaftMetadataStorage;
import se.mouaz.aegisdb.storage.metadata.RaftMetadataStorage;
import se.mouaz.aegisdb.storage.recovery.RecoveryManager;
import se.mouaz.aegisdb.storage.recovery.RecoveryResult;
import se.mouaz.aegisdb.storage.wal.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Top-level facade for local node storage and recovery (Master Project Plan §4, §8, §11).
 * Coordinates WAL, segment lifecycle, atomic metadata, and crash recovery.
 */
public class StorageEngine implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(StorageEngine.class);

    private final Path baseDataDir;
    private final Path walDir;
    private final Path metaDir;
    private final WalConfig walConfig;

    private final WalManager walManager;
    private final WalWriter walWriter;
    private final RaftMetadataStorage metadataStorage;
    private final RecoveryManager recoveryManager;
    private final StorageIndex storageIndex;

    public StorageEngine(Path baseDataDir, FsyncPolicy fsyncPolicy, long maxSegmentSizeBytes) throws IOException {
        this.baseDataDir = validateDataDir(baseDataDir);
        this.walDir = baseDataDir.resolve("wal");
        this.metaDir = baseDataDir.resolve("meta");

        Files.createDirectories(walDir);
        Files.createDirectories(metaDir);

        this.walConfig = WalConfig.builder()
                .walDir(walDir)
                .fsyncPolicy(fsyncPolicy)
                .maxSegmentSizeBytes(maxSegmentSizeBytes)
                .build();

        this.walManager = new WalManager(walConfig);
        this.walWriter = new WalWriter(walManager);
        this.metadataStorage = new FileRaftMetadataStorage(metaDir);
        this.recoveryManager = new RecoveryManager(metadataStorage, walManager);
        this.storageIndex = new StorageIndex();
    }

    public StorageEngine(Path baseDataDir) throws IOException {
        this(baseDataDir, FsyncPolicy.ALWAYS, WalConfig.DEFAULT_MAX_SEGMENT_SIZE_BYTES);
    }

    /**
     * Path security validation against directory traversal attacks (Master Project Plan §12).
     */
    public static Path validateDataDir(Path path) {
        Objects.requireNonNull(path, "baseDataDir cannot be null");
        Path normalized = path.normalize();
        if (normalized.toString().contains("..")) {
            throw new SecurityException("Unsafe path traversal detected in dataDir: " + path);
        }
        return normalized;
    }

    public Path baseDataDir() {
        return baseDataDir;
    }

    public Path walDir() {
        return walDir;
    }

    public Path metaDir() {
        return metaDir;
    }

    public WalManager walManager() {
        return walManager;
    }

    public WalWriter walWriter() {
        return walWriter;
    }

    public RaftMetadataStorage metadataStorage() {
        return metadataStorage;
    }

    public RecoveryManager recoveryManager() {
        return recoveryManager;
    }

    public StorageIndex storageIndex() {
        return storageIndex;
    }

    /**
     * Executes recovery and creates a durable Raft log populated with recovered state.
     */
    public DurableRecovery recoverAndCreateLog() throws IOException {
        RecoveryResult result = recoveryManager.recover();

        // Populate index from recovered entries
        DurableRaftLog log = new DurableRaftLog(walWriter, storageIndex, result.replayedEntries());
        return new DurableRecovery(result, log);
    }

    public record DurableRecovery(RecoveryResult result, DurableRaftLog raftLog) {}

    @Override
    public synchronized void close() throws IOException {
        log.info("Closing StorageEngine for {}", baseDataDir);
        walWriter.close();
        walManager.close();
    }
}
