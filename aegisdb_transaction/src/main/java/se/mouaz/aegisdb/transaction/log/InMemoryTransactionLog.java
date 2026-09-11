package se.mouaz.aegisdb.transaction.log;

import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.transaction.WriteOperation;
import se.mouaz.aegisdb.transaction.WriteSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deterministic in-memory transaction log implementation for ultra-fast unit testing (Master Project Plan §6 & §9).
 */
public class InMemoryTransactionLog implements TransactionLog {
    private final List<TransactionLogEntry> entries = new CopyOnWriteArrayList<>();
    private final AtomicLong sequenceGenerator = new AtomicLong(0);

    @Override
    public void logBegin(TransactionId txId, long timestamp) {
        long seq = sequenceGenerator.incrementAndGet();
        entries.add(TransactionLogEntry.begin(seq, txId, timestamp));
    }

    @Override
    public void logPrepare(TransactionId txId, long timestamp, WriteSet writeSet) {
        long seq = sequenceGenerator.incrementAndGet();
        List<WriteOperation> writes = (writeSet != null) ? new ArrayList<>(writeSet.operations().values()) : Collections.emptyList();
        entries.add(TransactionLogEntry.prepare(seq, txId, timestamp, writes));
    }

    @Override
    public void logCommitDecided(TransactionId txId, long timestamp, WriteSet writeSet) {
        long seq = sequenceGenerator.incrementAndGet();
        List<WriteOperation> writes = (writeSet != null) ? new ArrayList<>(writeSet.operations().values()) : Collections.emptyList();
        entries.add(TransactionLogEntry.commitDecided(seq, txId, timestamp, writes));
    }

    @Override
    public void logCommit(TransactionId txId, long commitTimestamp, WriteSet writeSet) {
        long seq = sequenceGenerator.incrementAndGet();
        List<WriteOperation> writes = (writeSet != null) ? new ArrayList<>(writeSet.operations().values()) : Collections.emptyList();
        entries.add(TransactionLogEntry.commit(seq, txId, commitTimestamp, writes));
    }

    @Override
    public void logAbort(TransactionId txId, long timestamp) {
        long seq = sequenceGenerator.incrementAndGet();
        entries.add(TransactionLogEntry.abort(seq, txId, timestamp));
    }

    @Override
    public List<TransactionLogEntry> replay() {
        return Collections.unmodifiableList(new ArrayList<>(entries));
    }

    @Override
    public long lastSequenceNumber() {
        return sequenceGenerator.get();
    }

    @Override
    public void close() {
        // no-op
    }
}
