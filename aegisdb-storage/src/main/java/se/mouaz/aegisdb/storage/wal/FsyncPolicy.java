package se.mouaz.aegisdb.storage.wal;

/**
 * Flush and fsync policy for Write-Ahead Log durability (Master Project Plan §8, §17).
 */
public enum FsyncPolicy {
    /**
     * fsync (FileChannel.force(true)) after every append or write batch.
     * Guarantees zero data loss on abrupt crash/power-loss.
     */
    ALWAYS,

    /**
     * Flush OS buffers periodically (background flusher), balancing throughput and durability.
     */
    PERIODIC,

    /**
     * Do not fsync automatically on write; sync only when explicitly requested by application.
     */
    MANUAL
}
