package se.mouaz.aegisdb.mvcc;

/**
 * Thrown when a concurrent write-write conflict or dirty write attempt occurs in MVCC.
 */
public class WriteConflictException extends MvccException {
    private final String key;
    private final long currentTxId;
    private final long conflictingTxId;

    public WriteConflictException(String key, long currentTxId, long conflictingTxId) {
        super("Write-write conflict on key '" + key + "': attempted by tx " + currentTxId +
                ", already locked or modified by active tx " + conflictingTxId);
        this.key = key;
        this.currentTxId = currentTxId;
        this.conflictingTxId = conflictingTxId;
    }

    public String key() {
        return key;
    }

    public long currentTxId() {
        return currentTxId;
    }

    public long conflictingTxId() {
        return conflictingTxId;
    }
}
