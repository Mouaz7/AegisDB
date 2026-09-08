package se.mouaz.aegisdb.storage.wal;

/**
 * Thrown when unrecoverable WAL corruption, invalid checksum, or bad framing is encountered (Master Project Plan §8).
 */
public class CorruptedWalException extends RuntimeException {

    public CorruptedWalException(String message) {
        super(message);
    }

    public CorruptedWalException(String message, Throwable cause) {
        super(message, cause);
    }
}
