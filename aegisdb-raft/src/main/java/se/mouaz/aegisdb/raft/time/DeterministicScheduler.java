package se.mouaz.aegisdb.raft.time;

import java.time.Duration;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DeterministicScheduler executes scheduled tasks synchronously as virtual time advances (Section 109).
 */
public class DeterministicScheduler implements Scheduler {
    private final TestClock clock;
    private final PriorityQueue<ScheduledItem> queue = new PriorityQueue<>();
    private final AtomicLong sequenceGenerator = new AtomicLong(0);

    public DeterministicScheduler(TestClock clock) {
        this.clock = clock;
    }

    public DeterministicScheduler() {
        this(new TestClock(0L));
    }

    public TestClock clock() {
        return clock;
    }

    @Override
    public synchronized CancellableTask schedule(Runnable task, Duration delay) {
        long runAt = clock.currentTimeMillis() + delay.toMillis();
        ScheduledItem item = new ScheduledItem(task, runAt, 0, sequenceGenerator.incrementAndGet());
        queue.add(item);
        return item;
    }

    @Override
    public synchronized CancellableTask scheduleAtFixedRate(Runnable task, Duration initialDelay, Duration period) {
        long runAt = clock.currentTimeMillis() + initialDelay.toMillis();
        ScheduledItem item = new ScheduledItem(task, runAt, period.toMillis(), sequenceGenerator.incrementAndGet());
        queue.add(item);
        return item;
    }

    public synchronized void advanceTime(Duration duration) {
        long targetTime = clock.currentTimeMillis() + duration.toMillis();
        while (!queue.isEmpty() && queue.peek().runAt <= targetTime) {
            ScheduledItem item = queue.poll();
            if (!item.cancelled.get()) {
                clock.setMillis(item.runAt);
                item.task.run();
                if (item.periodMillis > 0 && !item.cancelled.get()) {
                    item.runAt = item.runAt + item.periodMillis;
                    queue.add(item);
                }
            }
        }
        clock.setMillis(targetTime);
    }

    public synchronized void runNextPending() {
        if (!queue.isEmpty()) {
            ScheduledItem item = queue.poll();
            if (!item.cancelled.get()) {
                clock.setMillis(item.runAt);
                item.task.run();
                if (item.periodMillis > 0 && !item.cancelled.get()) {
                    item.runAt = item.runAt + item.periodMillis;
                    queue.add(item);
                }
            }
        }
    }

    public synchronized int pendingTasksCount() {
        return queue.size();
    }

    @Override
    public synchronized void close() {
        queue.clear();
    }

    private static class ScheduledItem implements CancellableTask, Comparable<ScheduledItem> {
        final Runnable task;
        long runAt;
        final long periodMillis;
        final long sequence;
        final AtomicBoolean cancelled = new AtomicBoolean(false);

        ScheduledItem(Runnable task, long runAt, long periodMillis, long sequence) {
            this.task = task;
            this.runAt = runAt;
            this.periodMillis = periodMillis;
            this.sequence = sequence;
        }

        @Override
        public void cancel() {
            cancelled.set(true);
        }

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }

        @Override
        public int compareTo(ScheduledItem other) {
            int cmp = Long.compare(this.runAt, other.runAt);
            if (cmp == 0) {
                return Long.compare(this.sequence, other.sequence);
            }
            return cmp;
        }
    }
}
