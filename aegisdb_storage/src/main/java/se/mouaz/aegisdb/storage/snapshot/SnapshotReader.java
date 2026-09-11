package se.mouaz.aegisdb.storage.snapshot;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Contract for reading point-in-time state machine snapshots (Master Project Plan §8, prepared for Phase 5).
 */
public interface SnapshotReader extends Closeable {

    record SnapshotMetadata(long lastIncludedIndex, long lastIncludedTerm, long checksum, Path path) {}
    record SnapshotReadResult(SnapshotMetadata metadata, byte[] data) {}

    /**
     * Loads the latest valid snapshot metadata if present.
     */
    Optional<SnapshotMetadata> loadLatestSnapshotMetadata() throws IOException;

    /**
     * Reads snapshot payload bytes.
     */
    byte[] readSnapshotData(Path snapshotPath) throws IOException;

    default Optional<SnapshotReadResult> readLatestSnapshot() throws IOException {
        Optional<SnapshotMetadata> metaOpt = loadLatestSnapshotMetadata();
        if (metaOpt.isEmpty()) {
            return Optional.empty();
        }
        SnapshotMetadata meta = metaOpt.get();
        byte[] data = readSnapshotData(meta.path());
        return Optional.of(new SnapshotReadResult(meta, data));
    }
}
