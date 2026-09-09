package se.mouaz.aegisdb.transaction;

/**
 * Base unchecked exception for all transaction-related failures (Master Project Plan §9 & §18).
 */
public class TransactionException extends RuntimeException {
    public TransactionException(String message) {
        super(message);
    }

    public TransactionException(String message, Throwable cause) {
        super(message, cause);
    }
}
