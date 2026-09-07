package se.mouaz.aegisdb.raft.time;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manually controlled Clock for deterministic testing (Section 109).
 */
public class TestClock implements Clock {
    private final AtomicLong millis;

    public TestClock(long initialMillis) {
        this.millis = new AtomicLong(initialMillis);
    }

    public TestClock() {
        this(0L);
    }

    @Override
    public long currentTimeMillis() {
        return millis.get();
    }

    public void advance(Duration duration) {
        millis.addAndGet(duration.toMillis());
    }

    public void advanceMillis(long ms) {
        millis.addAndGet(ms);
    }

    public void setMillis(long ms) {
        millis.set(ms);
    }
}
