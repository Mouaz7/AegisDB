package se.mouaz.aegisdb.transaction.distributed;

import se.mouaz.aegisdb.transaction.TransactionException;

/**
 * Thrown when a cross-shard distributed transaction is aborted during Two-Phase Commit.
 */
public class DistributedTransactionAbortedException extends TransactionException {
    public DistributedTransactionAbortedException(String message) {
        super(message);
    }

    public DistributedTransactionAbortedException(String message, Throwable cause) {
        super(message, cause);
    }
}
