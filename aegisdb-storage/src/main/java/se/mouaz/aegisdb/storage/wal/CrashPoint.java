package se.mouaz.aegisdb.storage.wal;

/**
 * Deterministic crash injection points for storage engine and WAL durability testing.
 *
 * <p><b>Design Note:</b> This mechanism provides deterministic crash injection at exact
 * I/O boundaries to prove WAL recovery correctness. It does not emulate physical hardware
 * power cuts or filesystem journal reordering semantics.
 */
public enum CrashPoint {
    /**
     * Crash immediately before the record framing header is written to disk.
     */
    BEFORE_RECORD_HEADER,

    /**
     * Crash after writing a partial record header (< 11 bytes framing).
     */
    PARTIAL_RECORD_HEADER,

    /**
     * Crash after writing the full header but only a partial payload fragment.
     */
    PARTIAL_PAYLOAD,

    /**
     * Crash after writing the complete record payload, but before fsync/force is executed.
     */
    BEFORE_FSYNC,

    /**
     * Crash during the execution of fsync/force.
     */
    DURING_FSYNC,

    /**
     * Crash after the WAL segment has been physically fsynced, but before Raft metadata is updated.
     */
    AFTER_FSYNC_BEFORE_METADATA_SAVE,

    /**
     * Crash during segment rotation when closing the active segment or allocating the next.
     */
    DURING_SEGMENT_ROLLOVER
}
