package se.mouaz.aegisdb.common;

/**
 * Unique identifier for an in-flight or committed transaction (Master Project Plan §6).
 */
public record TransactionId(long value) implements Comparable<TransactionId> {
    public TransactionId {
        if (value < 0) {
            throw new IllegalArgumentException("TransactionId value must be non-negative, got: " + value);
        }
    }

    public static TransactionId of(long value) {
        return new TransactionId(value);
    }

    @Override
    public int compareTo(TransactionId other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public String toString() {
        return "tx-" + value;
    }
}
