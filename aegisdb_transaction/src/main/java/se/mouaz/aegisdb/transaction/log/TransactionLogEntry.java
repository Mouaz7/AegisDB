package se.mouaz.aegisdb.transaction.log;

import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.transaction.TransactionState;
import se.mouaz.aegisdb.transaction.WriteOperation;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable transaction lifecycle log entry (Master Project Plan §9 & §18).
 */
public record TransactionLogEntry(
        long sequenceNumber,
        TransactionId txId,
        TransactionState state,
        long timestamp,
        List<WriteOperation> writeOperations
) {
    public TransactionLogEntry {
        Objects.requireNonNull(txId, "txId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        writeOperations = (writeOperations != null) ? List.copyOf(writeOperations) : Collections.emptyList();
    }

    public static TransactionLogEntry begin(long seq, TransactionId txId, long ts) {
        return new TransactionLogEntry(seq, txId, TransactionState.ACTIVE, ts, Collections.emptyList());
    }

    public static TransactionLogEntry prepare(long seq, TransactionId txId, long ts, List<WriteOperation> writes) {
        return new TransactionLogEntry(seq, txId, TransactionState.PREPARING, ts, writes);
    }

    public static TransactionLogEntry commit(long seq, TransactionId txId, long commitTs, List<WriteOperation> writes) {
        return new TransactionLogEntry(seq, txId, TransactionState.COMMITTED, commitTs, writes);
    }

    public static TransactionLogEntry abort(long seq, TransactionId txId, long ts) {
        return new TransactionLogEntry(seq, txId, TransactionState.ABORTED, ts, Collections.emptyList());
    }
}
