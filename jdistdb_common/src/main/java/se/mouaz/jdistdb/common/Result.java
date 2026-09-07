package se.mouaz.jdistdb.common;

import java.util.Objects;
import java.util.Optional;

public record Result<T>(boolean isSuccess, T value, ErrorCode errorCode, String errorMessage) {
    public static <T> Result<T> success(T value) {
        return new Result<>(true, value, null, null);
    }

    public static <T> Result<T> failure(ErrorCode code, String message) {
        Objects.requireNonNull(code, "ErrorCode cannot be null");
        return new Result<>(false, null, code, message);
    }

    public Optional<T> toOptional() {
        return isSuccess ? Optional.ofNullable(value) : Optional.empty();
    }
}
