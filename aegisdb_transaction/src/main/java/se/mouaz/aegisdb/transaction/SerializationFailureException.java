package se.mouaz.aegisdb.transaction;

/**
 * Thrown when a transaction violates serializability (e.g. Write Skew detected via read-set anti-dependency)
 * (Master Project Plan §9).
 */
public class SerializationFailureException extends TransactionException {
    private final String key;
    private final long txId;

    public SerializationFailureException(String message) {
        super(message);
        this.key = null;
        this.txId = -1L;
    }

    public SerializationFailureException(String key, long txId) {
        super(String.format("Serialization failure on key '%s': read version was concurrently modified after transaction %d started",
                key, txId));
        this.key = key;
        this.txId = txId;
    }

    public String getKey() {
        return key;
    }

    public long getTxId() {
        return txId;
    }
}
