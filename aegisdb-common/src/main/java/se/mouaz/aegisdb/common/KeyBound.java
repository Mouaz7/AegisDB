package se.mouaz.aegisdb.common;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

/**
 * Explicit representation of key range boundaries (§28, Phase 1 Multi-Raft).
 * Supports negative infinity (-∞), positive infinity (+∞), and concrete exact keys,
 * completely avoiding ambiguous empty-key sentinel collisons.
 */
public final class KeyBound implements Comparable<KeyBound>, Serializable {

    private static final long serialVersionUID = 1L;

    public enum Type {
        NEGATIVE_INFINITY,
        EXACT,
        POSITIVE_INFINITY
    }

    private static final KeyBound NEG_INF = new KeyBound(Type.NEGATIVE_INFINITY, null);
    private static final KeyBound POS_INF = new KeyBound(Type.POSITIVE_INFINITY, null);

    private final Type type;
    private final ByteArrayKey key;

    private KeyBound(Type type, ByteArrayKey key) {
        this.type = Objects.requireNonNull(type, "type cannot be null");
        this.key = key;
    }

    public static KeyBound negativeInfinity() {
        return NEG_INF;
    }

    public static KeyBound positiveInfinity() {
        return POS_INF;
    }

    public static KeyBound exact(ByteArrayKey key) {
        Objects.requireNonNull(key, "key cannot be null");
        return new KeyBound(Type.EXACT, key);
    }

    public static KeyBound exact(byte[] bytes) {
        return exact(ByteArrayKey.of(bytes));
    }

    public static KeyBound exact(String str) {
        return exact(ByteArrayKey.of(str));
    }

    public Type type() {
        return type;
    }

    public boolean isNegativeInfinity() {
        return type == Type.NEGATIVE_INFINITY;
    }

    public boolean isPositiveInfinity() {
        return type == Type.POSITIVE_INFINITY;
    }

    public boolean isExact() {
        return type == Type.EXACT;
    }

    public Optional<ByteArrayKey> key() {
        return Optional.ofNullable(key);
    }

    public ByteArrayKey requireKey() {
        if (key == null) {
            throw new IllegalStateException("Bound is " + type + ", has no exact key");
        }
        return key;
    }

    @Override
    public int compareTo(KeyBound other) {
        if (this == other) {
            return 0;
        }
        Objects.requireNonNull(other, "other cannot be null");

        if (this.type == other.type) {
            if (this.type == Type.EXACT) {
                return this.key.compareTo(other.key);
            }
            return 0;
        }

        if (this.type == Type.NEGATIVE_INFINITY) {
            return -1;
        }
        if (other.type == Type.NEGATIVE_INFINITY) {
            return 1;
        }
        if (this.type == Type.POSITIVE_INFINITY) {
            return 1;
        }
        if (other.type == Type.POSITIVE_INFINITY) {
            return -1;
        }

        throw new IllegalStateException("Unreachable comparison between " + this + " and " + other);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KeyBound other)) {
            return false;
        }
        if (this.type != other.type) {
            return false;
        }
        return Objects.equals(this.key, other.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key);
    }

    @Override
    public String toString() {
        return switch (type) {
            case NEGATIVE_INFINITY -> "-∞";
            case POSITIVE_INFINITY -> "+∞";
            case EXACT -> key.toString();
        };
    }
}
