package se.mouaz.aegisdb.storage.snapshot;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Contract for reading point-in-time state machine snapshots (Master Project Plan §8, prepared for Sprint 5).
 */
public interface SnapshotReader extends Closeable {

    record SnapshotMetadata(long lastIncludedIndex, long lastIncludedTerm, long checksum, Path path) {}

    /**
     * Loads the latest valid snapshot metadata if present.
     */
    Optional<SnapshotMetadata> loadLatestSnapshotMetadata() throws IOException;

    /**
     * Reads snapshot payload bytes.
     */
    byte[] readSnapshotData(Path snapshotPath) throws IOException;
}
