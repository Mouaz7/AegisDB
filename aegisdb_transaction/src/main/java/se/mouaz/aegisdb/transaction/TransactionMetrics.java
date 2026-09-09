package se.mouaz.aegisdb.transaction;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Operational diagnostics and telemetry metrics for the Transaction Engine (Master Project Plan §15).
 */
public class TransactionMetrics {
    private final AtomicLong transactionCount = new AtomicLong(0);
    private final AtomicLong commitCount = new AtomicLong(0);
    private final AtomicLong abortCount = new AtomicLong(0);
    private final AtomicLong writeConflictCount = new AtomicLong(0);
    private final AtomicLong serializationFailureCount = new AtomicLong(0);
    private final AtomicLong timeoutCount = new AtomicLong(0);

    public void incrementStarted() {
        transactionCount.incrementAndGet();
    }

    public void incrementCommitted() {
        commitCount.incrementAndGet();
    }

    public void incrementAborted() {
        abortCount.incrementAndGet();
    }

    public void incrementWriteConflict() {
        writeConflictCount.incrementAndGet();
    }

    public void incrementSerializationFailure() {
        serializationFailureCount.incrementAndGet();
    }

    public void incrementTimeout() {
        timeoutCount.incrementAndGet();
    }

    public long transactionCount() {
        return transactionCount.get();
    }

    public long commitCount() {
        return commitCount.get();
    }

    public long abortCount() {
        return abortCount.get();
    }

    public long writeConflictCount() {
        return writeConflictCount.get();
    }

    public long serializationFailureCount() {
        return serializationFailureCount.get();
    }

    public long timeoutCount() {
        return timeoutCount.get();
    }

    public void reset() {
        transactionCount.set(0);
        commitCount.set(0);
        abortCount.set(0);
        writeConflictCount.set(0);
        serializationFailureCount.set(0);
        timeoutCount.set(0);
    }
}
