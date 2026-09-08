package se.mouaz.aegisdb.mvcc;

/**
 * Base unchecked exception for MVCC concurrency and storage operations.
 */
public class MvccException extends RuntimeException {
    public MvccException(String message) {
        super(message);
    }

    public MvccException(String message, Throwable cause) {
        super(message, cause);
    }
}
