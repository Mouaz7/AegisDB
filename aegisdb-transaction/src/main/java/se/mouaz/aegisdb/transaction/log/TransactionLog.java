package se.mouaz.aegisdb.transaction.log;

import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.transaction.WriteSet;

import java.io.Closeable;
import java.util.List;

/**
 * Journal interface for durable transaction lifecycle events (Master Project Plan §9 & §18).
 */
public interface TransactionLog extends Closeable {
    void logBegin(TransactionId txId, long timestamp);
    void logPrepare(TransactionId txId, long timestamp, WriteSet writeSet);
    void logCommitDecided(TransactionId txId, long timestamp, WriteSet writeSet);
    void logCommit(TransactionId txId, long commitTimestamp, WriteSet writeSet);
    void logAbort(TransactionId txId, long timestamp);
    List<TransactionLogEntry> replay();
    long lastSequenceNumber();
    @Override
    void close();
}
