package se.mouaz.aegisdb.transaction;

/**
 * Thrown when a transaction exceeds its configured lifetime TTL (Master Project Plan §12).
 */
public class TransactionTimeoutException extends TransactionException {
    private final long txId;
    private final long elapsedMillis;
    private final long ttlMillis;

    public TransactionTimeoutException(long txId, long elapsedMillis, long ttlMillis) {
        super(String.format("Transaction %d expired: elapsed %d ms exceeded TTL of %d ms",
                txId, elapsedMillis, ttlMillis));
        this.txId = txId;
        this.elapsedMillis = elapsedMillis;
        this.ttlMillis = ttlMillis;
    }

    public long getTxId() {
        return txId;
    }

    public long getElapsedMillis() {
        return elapsedMillis;
    }

    public long getTtlMillis() {
        return ttlMillis;
    }
}
