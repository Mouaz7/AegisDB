package se.mouaz.aegisdb.common;

import java.util.Objects;

/**
 * Unique identifier for a database client (Master Project Plan §6 & §10).
 * Used in combination with RequestId for request deduplication and idempotency.
 */
public record ClientId(String value) implements Comparable<ClientId> {
    public ClientId {
        Objects.requireNonNull(value, "ClientId value must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("ClientId value must not be blank");
        }
    }

    public static ClientId of(String value) {
        return new ClientId(value);
    }

    @Override
    public int compareTo(ClientId other) {
        return this.value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
