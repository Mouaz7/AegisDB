package se.mouaz.aegisdb.chaos;

/**
 * Categorization of network and node faults that can be injected
 * by the AegisDB Chaos subsystem.
 */
public enum FaultType {
    /**
     * Drops RPC requests or responses silently.
     */
    DROP,

    /**
     * Delays delivery of RPC requests or responses by a specified duration.
     */
    DELAY,

    /**
     * Clones and delivers the RPC multiple times to simulate network echo / retransmissions.
     */
    DUPLICATE,

    /**
     * Corrupts payload contents or metadata to simulate bit-flips or wire corruption.
     */
    CORRUPT,

    /**
     * Completely severs communication between designated partition groups.
     */
    PARTITION
}
