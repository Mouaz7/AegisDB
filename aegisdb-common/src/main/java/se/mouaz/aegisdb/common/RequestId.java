package se.mouaz.aegisdb.common;

import java.util.Objects;

/**
 * Unique request sequence identifier per client (Master Project Plan §6 & §10).
 * Combined with ClientId to enforce exactly-once execution semantics and idempotency.
 */
public record RequestId(long sequenceNumber) implements Comparable<RequestId> {
    public RequestId {
        if (sequenceNumber < 0) {
            throw new IllegalArgumentException("RequestId sequenceNumber must be non-negative, got: " + sequenceNumber);
        }
    }

    public static RequestId of(long sequenceNumber) {
        return new RequestId(sequenceNumber);
    }

    @Override
    public int compareTo(RequestId other) {
        return Long.compare(this.sequenceNumber, other.sequenceNumber);
    }

    @Override
    public String toString() {
        return "req-" + sequenceNumber;
    }
}
