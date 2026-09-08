package se.mouaz.aegisdb.storage.snapshot;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Contract for creating point-in-time state machine snapshots (Master Project Plan §8, prepared for Sprint 5).
 */
public interface SnapshotWriter extends Closeable {

    /**
     * Writes snapshot data for the given snapshot metadata.
     */
    Path writeSnapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] stateMachineData) throws IOException;
}
