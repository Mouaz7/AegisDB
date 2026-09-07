package se.mouaz.aegisdb.raft.time;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SystemScheduler implements Scheduler {
    private final ScheduledExecutorService executor;

    public SystemScheduler() {
        this.executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-system-scheduler");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public CancellableTask schedule(Runnable task, Duration delay) {
        ScheduledFuture<?> future = executor.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
        return new CancellableTask() {
            @Override
            public void cancel() {
                future.cancel(false);
            }

            @Override
            public boolean isCancelled() {
                return future.isCancelled();
            }
        };
    }

    @Override
    public CancellableTask scheduleAtFixedRate(Runnable task, Duration initialDelay, Duration period) {
        ScheduledFuture<?> future = executor.scheduleAtFixedRate(task, initialDelay.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
        return new CancellableTask() {
            @Override
            public void cancel() {
                future.cancel(false);
            }

            @Override
            public boolean isCancelled() {
                return future.isCancelled();
            }
        };
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
