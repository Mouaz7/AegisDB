package se.mouaz.aegisdb.storage.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Common contract for a storage segment (Master Project Plan §8).
 */
public interface StorageSegment extends Closeable {

    /**
     * Unique monotonically increasing identifier of the segment.
     */
    long segmentId();

    /**
     * Physical file path on disk.
     */
    Path path();

    /**
     * Current size of the segment file in bytes.
     */
    long size();

    /**
     * Whether the segment has been sealed (read-only).
     */
    boolean isSealed();

    /**
     * Seals the segment, preventing further appends.
     */
    void seal() throws IOException;
}
