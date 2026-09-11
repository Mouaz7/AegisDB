package se.mouaz.aegisdb.raft.time;

import java.time.Duration;
import java.time.Instant;

/**
 * Clock abstraction to allow deterministic testing (Section 109).
 */
public interface Clock {
    long currentTimeMillis();

    default Instant now() {
        return Instant.ofEpochMilli(currentTimeMillis());
    }

    static Clock system() {
        return new SystemClock();
    }

    static TestClock test(long initialMillis) {
        return new TestClock(initialMillis);
    }
}
