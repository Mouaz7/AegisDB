package se.mouaz.aegisdb.transaction;

/**
 * Transaction isolation levels supported by AegisDB (Master Project Plan §9).
 * - SNAPSHOT_ISOLATION: Core guarantee preventing Dirty Read, Lost Update, Non-Repeatable Read.
 * - SERIALIZABLE: Advanced guarantee preventing Write Skew via read-set anti-dependency checking.
 */
public enum IsolationLevel {
    SNAPSHOT_ISOLATION,
    SERIALIZABLE
}
