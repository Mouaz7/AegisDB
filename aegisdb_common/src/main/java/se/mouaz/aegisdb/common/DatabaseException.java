package se.mouaz.aegisdb.common;

import java.util.Objects;

public class DatabaseException extends RuntimeException {
    private final ErrorCode errorCode;

    public DatabaseException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode cannot be null");
    }

    public DatabaseException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = Objects.requireNonNull(errorCode, "errorCode cannot be null");
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
