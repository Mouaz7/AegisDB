package se.mouaz.aegisdb.mvcc;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents an immutable point-in-time snapshot view for consistent reads (Master Plan §9).
 * Implements {@link AutoCloseable} to allow safe registration and deregistration with the
 * MVCC garbage collection active snapshot tracker.
 */
public class Snapshot implements AutoCloseable {
    private final long snapshotId;
    private final long readTimestamp;
    private final long readerTxId;
    private final Set<Long> activeTxIds;
    private final Runnable closeCallback;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public Snapshot(long snapshotId, long readTimestamp, long readerTxId, Set<Long> activeTxIds, Runnable closeCallback) {
        this.snapshotId = snapshotId;
        this.readTimestamp = readTimestamp;
        this.readerTxId = readerTxId;
        this.activeTxIds = activeTxIds == null ? Set.of() : Set.copyOf(activeTxIds);
        this.closeCallback = closeCallback;
    }

    public static Snapshot of(long readTimestamp) {
        return new Snapshot(readTimestamp, readTimestamp, 0L, Set.of(), null);
    }

    public static Snapshot of(long readTimestamp, Set<Long> activeTxIds) {
        return new Snapshot(readTimestamp, readTimestamp, 0L, activeTxIds, null);
    }

    public static Snapshot forTransaction(long readTimestamp, long txId, Set<Long> activeTxIds) {
        return new Snapshot(readTimestamp, readTimestamp, txId, activeTxIds, null);
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long readTimestamp() {
        return readTimestamp;
    }

    public long readerTxId() {
        return readerTxId;
    }

    public Set<Long> activeTxIds() {
        return activeTxIds;
    }

    public boolean isTxActiveAtSnapshot(long txId) {
        return activeTxIds.contains(txId);
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            if (closeCallback != null) {
                closeCallback.run();
            }
        }
    }

    @Override
    public String toString() {
        return "Snapshot{" +
                "id=" + snapshotId +
                ", readTimestamp=" + readTimestamp +
                ", readerTxId=" + readerTxId +
                ", activeTxCount=" + activeTxIds.size() +
                ", closed=" + closed.get() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Snapshot snapshot = (Snapshot) o;
        return snapshotId == snapshot.snapshotId && readTimestamp == snapshot.readTimestamp && readerTxId == snapshot.readerTxId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, readTimestamp, readerTxId);
    }
}
