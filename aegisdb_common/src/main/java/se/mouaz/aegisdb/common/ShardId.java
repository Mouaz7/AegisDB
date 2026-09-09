package se.mouaz.aegisdb.common;

import java.util.Objects;

/**
 * Strongly-typed unique identifier for a database shard (Master Project Plan §6, §10; US013, US014).
 * Immutable value object with natural sorting and string/index factory methods.
 */
public record ShardId(String value) implements Comparable<ShardId> {

    public ShardId {
        Objects.requireNonNull(value, "ShardId value cannot be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("ShardId value cannot be blank");
        }
    }

    public static ShardId of(String value) {
        return new ShardId(value);
    }

    public static ShardId of(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Shard index cannot be negative, got: " + index);
        }
        return new ShardId("shard-" + index);
    }

    @Override
    public int compareTo(ShardId other) {
        return this.value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
