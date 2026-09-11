package se.mouaz.aegisdb.mvcc;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates monotonically increasing logical timestamps for MVCC transactions and snapshots (Master Plan §9).
 */
public class TimestampProvider {
    private final AtomicLong current = new AtomicLong(0);

    public TimestampProvider() {
        this(0);
    }

    public TimestampProvider(long initialTimestamp) {
        this.current.set(initialTimestamp);
    }

    /**
     * Advances and returns the next monotonic logical timestamp.
     */
    public long nextTimestamp() {
        return current.incrementAndGet();
    }

    /**
     * Returns the latest allocated logical timestamp.
     */
    public long currentTimestamp() {
        return current.get();
    }
}
