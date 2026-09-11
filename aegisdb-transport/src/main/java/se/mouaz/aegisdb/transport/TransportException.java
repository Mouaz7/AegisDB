package se.mouaz.aegisdb.transport;

import se.mouaz.aegisdb.common.DatabaseException;
import se.mouaz.aegisdb.common.ErrorCode;

public class TransportException extends DatabaseException {
    public TransportException(ErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public TransportException(ErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }

    public static TransportException timeout(String message) {
        return new TransportException(ErrorCode.TIMEOUT, message);
    }

    public static TransportException nodeNotFound(String message) {
        return new TransportException(ErrorCode.NODE_NOT_FOUND, message);
    }

    public static TransportException nodeStopped(String message) {
        return new TransportException(ErrorCode.NODE_STOPPED, message);
    }

    public static TransportException networkError(String message, Throwable cause) {
        return new TransportException(ErrorCode.NETWORK_ERROR, message, cause);
    }
}
