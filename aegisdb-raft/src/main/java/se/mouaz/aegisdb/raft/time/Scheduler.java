package se.mouaz.aegisdb.raft.time;

import java.time.Duration;

/**
 * Scheduler abstraction to enable deterministic execution of timeouts (Section 109).
 */
public interface Scheduler extends AutoCloseable {
    CancellableTask schedule(Runnable task, Duration delay);

    CancellableTask scheduleAtFixedRate(Runnable task, Duration initialDelay, Duration period);

    @Override
    void close();

    interface CancellableTask {
        void cancel();
        boolean isCancelled();
    }
}
