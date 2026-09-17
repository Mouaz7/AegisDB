package se.mouaz.aegisdb.chaos.linearizability;

/**
 * Execution status of an operation observed by a client.
 */
public enum OperationStatus {
    /**
     * Operation definitively acknowledged by cluster with an observed value.
     */
    OK,

    /**
     * Operation experienced timeout, network partition, or disconnect.
     * The operation may have taken effect prior to timeout or may have been dropped.
     * The linearizability checker evaluates both possibilities.
     */
    TIMEOUT_INDETERMINATE,

    /**
     * Operation definitively rejected or failed (e.g. key missing, explicit CAS failure).
     */
    FAIL
}
