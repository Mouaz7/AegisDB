package se.mouaz.aegisdb.mvcc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Garbage collector for MVCC multi-version chains (Master Plan §9).
 * <p>
 * Enforces the primary safety invariant:
 * "Garbage collection must not delete a version still visible to an active snapshot."
 * <p>
 * Pruning is safe because it uses the minimum active snapshot read timestamp
 * as the pruning watermark. Any version required by an active snapshot is retained.
 */
public class MvccGarbageCollector implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MvccGarbageCollector.class);

    private final MvccStore store;
    private ScheduledExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Statistics reported after a garbage collection sweep.
     */
    public record GcStats(
            long versionsReclaimed,
            long keysReclaimed,
            long watermarkTimestamp,
            long durationNanos
    ) {
        public double durationMs() {
            return durationNanos / 1_000_000.0;
        }
    }

    public MvccGarbageCollector(MvccStore store) {
        this.store = Objects.requireNonNull(store, "store must not be null");
    }

    /**
     * Runs a synchronous garbage collection sweep across all version chains in the store.
     *
     * @return GC run statistics
     */
    public GcStats collectGarbage() {
        long startTime = System.nanoTime();
        long watermark = store.minActiveSnapshotTimestamp();

        long versionsReclaimed = 0;
        long keysReclaimed = 0;

        for (var entry : store.chains().entrySet()) {
            String key = entry.getKey();
            VersionChain chain = entry.getValue();

            int pruned = chain.pruneOlderThan(watermark);
            versionsReclaimed += pruned;

            if (store.removeIfObsolete(key)) {
                keysReclaimed++;
            }
        }

        long durationNanos = System.nanoTime() - startTime;
        GcStats stats = new GcStats(versionsReclaimed, keysReclaimed, watermark, durationNanos);

        if (versionsReclaimed > 0 || keysReclaimed > 0) {
            log.info("MVCC GC completed: reclaimed {} versions, {} keys at watermark {} in {} ms",
                    versionsReclaimed, keysReclaimed, watermark, String.format("%.2f", stats.durationMs()));
        } else {
            log.debug("MVCC GC completed: 0 versions reclaimed at watermark {}", watermark);
        }

        return stats;
    }

    /**
     * Starts a background thread running periodic garbage collection sweeps.
     *
     * @param periodMs interval between GC sweeps in milliseconds
     */
    public synchronized void start(long periodMs) {
        if (running.compareAndSet(false, true)) {
            executor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "aegisdb-mvcc-gc");
                t.setDaemon(true);
                return t;
            });
            executor.scheduleWithFixedDelay(() -> {
                try {
                    collectGarbage();
                } catch (Throwable t) {
                    log.error("Unexpected error during background MVCC garbage collection", t);
                }
            }, periodMs, periodMs, TimeUnit.MILLISECONDS);
            log.info("Started background MVCC garbage collector with interval {} ms", periodMs);
        }
    }

    /**
     * Stops the background GC thread if running.
     */
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            if (executor != null) {
                executor.shutdownNow();
                try {
                    if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                        log.warn("MVCC GC executor did not terminate in time");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            log.info("Stopped background MVCC garbage collector");
        }
    }

    @Override
    public void close() {
        stop();
    }
}
