package se.mouaz.aegisdb.transaction;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable representation of a write operation in a transaction WriteSet.
 */
public record WriteOperation(String key, OperationType type, byte[] value) {
    public WriteOperation {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(type, "type must not be null");
        value = (value != null) ? Arrays.copyOf(value, value.length) : new byte[0];
    }

    public static WriteOperation put(String key, byte[] value) {
        return new WriteOperation(key, OperationType.PUT, value);
    }

    public static WriteOperation delete(String key) {
        return new WriteOperation(key, OperationType.DELETE, new byte[0]);
    }

    @Override
    public byte[] value() {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WriteOperation that)) return false;
        return Objects.equals(key, that.key) &&
               type == that.type &&
               Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(key, type);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
