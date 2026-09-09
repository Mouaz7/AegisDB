package se.mouaz.aegisdb.transaction;

import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.mvcc.VersionChain;
import se.mouaz.aegisdb.mvcc.VersionedValue;

import java.util.Map;
import java.util.Objects;

/**
 * Evaluates concurrency conflicts for transactions (Master Project Plan §9 & §18).
 * Enforces:
 * 1. First-Committer-Wins write-write conflict detection (Snapshot Isolation).
 * 2. Read-Set anti-dependency validation (Serializable Snapshot Isolation).
 */
public class ConflictDetector {

    /**
     * Checks for write-write conflicts on all keys in the write set.
     *
     * @param context active transaction context
     * @param store   underlying MVCC store
     * @throws WriteConflictException if any write-write conflict is detected
     */
    public void validateWriteConflicts(TransactionContext context, MvccStore store) {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(store, "store must not be null");

        long txId = context.id().value();
        long startTimestamp = context.startTimestamp();

        for (String key : context.writeSet().keys()) {
            VersionChain chain = store.chains().get(key);
            if (chain == null) {
                continue;
            }

            VersionedValue node = chain.head();
            // Check for concurrent uncommitted write locks/versions
            while (node != null && !node.isCommitted()) {
                if (node.createTxId() != txId) {
                    throw new WriteConflictException(key, txId, node.createTxId());
                }
                node = node.next();
            }

            // Check if committed node was committed after this transaction started
            // or was committed by a transaction that was in-flight at this transaction's start
            if (node != null && node.createTxId() != txId) {
                boolean committedAfterStart = node.commitTimestamp() > startTimestamp;
                boolean committedByInFlight = context.snapshot().activeTxIds().contains(node.createTxId());
                if (committedAfterStart || committedByInFlight) {
                    throw new WriteConflictException(key, txId, node.createTxId());
                }
            }
        }
    }

    /**
     * Checks for serialization anomalies (e.g. Write Skew) by verifying that no key in the read set
     * has been committed or modified by another transaction since this transaction read it.
     *
     * @param context active transaction context
     * @param store   underlying MVCC store
     * @throws SerializationFailureException if a read-set anti-dependency is detected
     */
    public void validateSerializableConflicts(TransactionContext context, MvccStore store) {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(store, "store must not be null");

        if (context.isolationLevel() != IsolationLevel.SERIALIZABLE) {
            return;
        }

        long txId = context.id().value();
        long startTimestamp = context.startTimestamp();

        for (Map.Entry<String, Long> entry : context.readSet().entries().entrySet()) {
            String key = entry.getKey();
            long observedTs = entry.getValue();

            VersionChain chain = store.chains().get(key);
            if (chain == null) {
                if (observedTs != 0L) {
                    throw new SerializationFailureException(key, txId);
                }
                continue;
            }

            // Find newest committed version
            VersionedValue node = chain.head();
            while (node != null && !node.isCommitted()) {
                node = node.next();
            }

            if (observedTs == 0L) {
                // Key did not exist when read; conflict if a committed version now exists
                if (node != null && node.commitTimestamp() > startTimestamp && node.createTxId() != txId) {
                    throw new SerializationFailureException(key, txId);
                }
            } else {
                // Key existed; conflict if a newer committed version exists
                if (node != null && node.commitTimestamp() > observedTs && node.createTxId() != txId) {
                    throw new SerializationFailureException(key, txId);
                }
            }
        }
    }
}
