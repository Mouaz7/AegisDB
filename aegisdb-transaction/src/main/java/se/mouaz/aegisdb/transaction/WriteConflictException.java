package se.mouaz.aegisdb.transaction;

/**
 * Thrown when a concurrent write conflict occurs under First-Committer-Wins (Master Project Plan §9).
 */
public class WriteConflictException extends TransactionException {
    private final String key;
    private final long txId;
    private final long conflictingTxId;

    public WriteConflictException(String key, long txId, long conflictingTxId) {
        super(String.format("Write conflict on key '%s': transaction %d conflicted with transaction %d",
                key, txId, conflictingTxId));
        this.key = key;
        this.txId = txId;
        this.conflictingTxId = conflictingTxId;
    }

    public String getKey() {
        return key;
    }

    public long getTxId() {
        return txId;
    }

    public long getConflictingTxId() {
        return conflictingTxId;
    }
}
