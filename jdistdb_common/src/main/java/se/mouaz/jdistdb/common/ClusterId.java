package se.mouaz.jdistdb.common;

import java.util.Objects;

public record ClusterId(String value) {
    public ClusterId {
        Objects.requireNonNull(value, "ClusterId value cannot be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("ClusterId value cannot be blank");
        }
    }

    public static ClusterId of(String value) {
        return new ClusterId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
