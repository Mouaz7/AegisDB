package se.mouaz.jdistdb.common;

import java.util.Objects;

public record NodeId(String value) {
    public NodeId {
        Objects.requireNonNull(value, "NodeId value cannot be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("NodeId value cannot be blank");
        }
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
