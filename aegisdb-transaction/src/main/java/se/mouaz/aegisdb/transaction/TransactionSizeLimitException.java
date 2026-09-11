package se.mouaz.aegisdb.transaction;

/**
 * Thrown when a transaction exceeds the maximum allowed number of keys in its write set (Master Project Plan §12).
 */
public class TransactionSizeLimitException extends TransactionException {
    public TransactionSizeLimitException(String message) {
        super(message);
    }
}
