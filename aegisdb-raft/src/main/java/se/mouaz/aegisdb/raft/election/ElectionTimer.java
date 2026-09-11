package se.mouaz.aegisdb.raft.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.raft.time.Clock;
import se.mouaz.aegisdb.raft.time.Scheduler;

import java.time.Duration;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ElectionTimer manages randomized election timeouts (Section 19).
 */
public class ElectionTimer {
    private static final Logger log = LoggerFactory.getLogger(ElectionTimer.class);

    private final Scheduler scheduler;
    private final Clock clock;
    private final Duration minTimeout;
    private final Duration maxTimeout;
    private final Random random;
    private final Runnable onTimeoutCallback;

    private Scheduler.CancellableTask currentTask;
    private final AtomicLong lastResetTimestamp = new AtomicLong(0);

    public ElectionTimer(Scheduler scheduler,
                         Clock clock,
                         Duration minTimeout,
                         Duration maxTimeout,
                         Random random,
                         Runnable onTimeoutCallback) {
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler cannot be null");
        this.clock = Objects.requireNonNull(clock, "clock cannot be null");
        this.minTimeout = minTimeout != null ? minTimeout : Duration.ofMillis(150);
        this.maxTimeout = maxTimeout != null ? maxTimeout : Duration.ofMillis(300);
        this.random = random != null ? random : new Random();
        this.onTimeoutCallback = Objects.requireNonNull(onTimeoutCallback, "onTimeoutCallback cannot be null");
    }

    public synchronized void reset() {
        cancel();
        long delayMillis;
        long min = minTimeout.toMillis();
        long max = maxTimeout.toMillis();
        if (max > min) {
            delayMillis = min + random.nextLong(max - min);
        } else {
            delayMillis = min;
        }

        lastResetTimestamp.set(clock.currentTimeMillis());
        currentTask = scheduler.schedule(() -> {
            log.debug("Election timeout expired after {} ms", delayMillis);
            onTimeoutCallback.run();
        }, Duration.ofMillis(delayMillis));
    }

    public synchronized void resetWithDuration(Duration fixedDuration) {
        cancel();
        lastResetTimestamp.set(clock.currentTimeMillis());
        currentTask = scheduler.schedule(onTimeoutCallback, fixedDuration);
    }

    public synchronized void cancel() {
        if (currentTask != null && !currentTask.isCancelled()) {
            currentTask.cancel();
            currentTask = null;
        }
    }

    public long lastResetTimestamp() {
        return lastResetTimestamp.get();
    }
}
