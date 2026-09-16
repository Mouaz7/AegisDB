package se.mouaz.aegisdb.chaos.linearizability;

/**
 * Supported operations for sequential consistency / linearizability modeling.
 */
public enum OperationType {
    READ,
    WRITE,
    CAS
}
