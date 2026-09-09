package se.mouaz.aegisdb.transaction.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.transaction.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Local shard participant backed by a TransactionManager (Master Project Plan §10; US015, Milestone M4 Gate).
 * Acquires exclusive prepare locks on write-set keys during Phase 1,
 * enforces serializability against concurrent local transactions, and implements idempotent 2PC handlers.
 */
public class LocalShardParticipant implements TransactionParticipant {
    private static final Logger log = LoggerFactory.getLogger(LocalShardParticipant.class);

    private record PreparedContext(
            TransactionId txId,
            Transaction tx,
            PrepareRequest request,
            Set<String> lockedKeys
    ) {}

    private final ShardId shardId;
    private final TransactionManager transactionManager;
    private final ReentrantLock lock = new ReentrantLock();

    // In-flight prepared transactions
    private final Map<TransactionId, PreparedContext> inDoubtTransactions = new ConcurrentHashMap<>();
    // Global lock registry for write keys on this shard
    private final Map<String, TransactionId> activeKeyLocks = new ConcurrentHashMap<>();
    // Terminal idempotency tracking
    private final Set<TransactionId> committedTransactions = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<TransactionId> abortedTransactions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public LocalShardParticipant(ShardId shardId, TransactionManager transactionManager) {
        this.shardId = Objects.requireNonNull(shardId, "shardId cannot be null");
        this.transactionManager = Objects.requireNonNull(transactionManager, "transactionManager cannot be null");
    }

    @Override
    public CompletableFuture<ParticipantVote> prepare(PrepareRequest request) {
        Objects.requireNonNull(request, "request cannot be null");
        TransactionId txId = request.txId();

        // 1. Check idempotency: If already prepared, re-affirm YES
        if (inDoubtTransactions.containsKey(txId)) {
            return CompletableFuture.completedFuture(ParticipantVote.PREPARED);
        }
        if (abortedTransactions.contains(txId)) {
            return CompletableFuture.completedFuture(ParticipantVote.ABORT);
        }
        if (committedTransactions.contains(txId)) {
            return CompletableFuture.completedFuture(ParticipantVote.PREPARED);
        }

        lock.lock();
        try {
            // 2. OCC / Anti-dependency validation on read keys
            for (Map.Entry<String, Optional<byte[]>> entry : request.expectedReads().entrySet()) {
                String readKey = entry.getKey();
                Optional<byte[]> expected = entry.getValue();

                // Check if key is locked by another in-doubt transaction
                TransactionId holder = activeKeyLocks.get(readKey);
                if (holder != null && !holder.equals(txId)) {
                    log.warn("Shard {} PREPARE rejected for {}: Read key '{}' locked by in-doubt tx {}",
                            shardId, txId, readKey, holder);
                    abortedTransactions.add(txId);
                    return CompletableFuture.completedFuture(ParticipantVote.ABORT);
                }

                // Check committed store value
                Optional<byte[]> current = transactionManager.mvccStore().get(readKey);
                boolean matches;
                if (expected == null || expected.isEmpty()) {
                    matches = current.isEmpty();
                } else {
                    matches = current.isPresent() && Arrays.equals(expected.get(), current.get());
                }

                if (!matches) {
                    log.warn("Shard {} PREPARE rejected for {}: Stale read on key '{}' (OCC serializability violation)",
                            shardId, txId, readKey);
                    abortedTransactions.add(txId);
                    return CompletableFuture.completedFuture(ParticipantVote.ABORT);
                }
            }

            // 3. Conflict detection on active key locks
            Set<String> targetKeys = new HashSet<>();
            for (WriteOperation op : request.writes()) {
                targetKeys.add(op.key());
                TransactionId holder = activeKeyLocks.get(op.key());
                if (holder != null && !holder.equals(txId)) {
                    log.warn("Shard {} PREPARE rejected for {}: Key '{}' locked by in-doubt tx {}",
                            shardId, txId, op.key(), holder);
                    abortedTransactions.add(txId);
                    return CompletableFuture.completedFuture(ParticipantVote.ABORT);
                }
            }

            // 4. Acquire exclusive prepare locks
            for (String k : targetKeys) {
                activeKeyLocks.put(k, txId);
            }

            // 5. Start local transaction and validate against MVCC
            Transaction localTx = transactionManager.beginTransaction(IsolationLevel.SERIALIZABLE);
            try {
                for (WriteOperation op : request.writes()) {
                    if (op.type() == OperationType.PUT) {
                        localTx.put(op.key(), op.value());
                    } else if (op.type() == OperationType.DELETE) {
                        localTx.delete(op.key());
                    }
                }
                localTx.prepare();

                inDoubtTransactions.put(txId, new PreparedContext(txId, localTx, request, targetKeys));
                log.info("Shard {} successfully PREPARED transaction {}", shardId, txId);
                return CompletableFuture.completedFuture(ParticipantVote.PREPARED);

            } catch (Exception e) {
                log.warn("Shard {} local prepare failed for {}: {}", shardId, txId, e.getMessage());
                // Release locks and abort local tx
                for (String k : targetKeys) {
                    activeKeyLocks.remove(k, txId);
                }
                localTx.abort();
                abortedTransactions.add(txId);
                return CompletableFuture.completedFuture(ParticipantVote.ABORT);
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> commit(TransactionId txId) {
        Objects.requireNonNull(txId, "txId cannot be null");

        // Idempotent commit check
        if (committedTransactions.contains(txId)) {
            return CompletableFuture.completedFuture(null);
        }

        lock.lock();
        try {
            PreparedContext ctx = inDoubtTransactions.remove(txId);
            if (ctx != null) {
                try {
                    ctx.tx().commit();
                    log.info("Shard {} COMMITTED transaction {}", shardId, txId);
                } finally {
                    for (String k : ctx.lockedKeys()) {
                        activeKeyLocks.remove(k, txId);
                    }
                    committedTransactions.add(txId);
                }
            } else {
                // If not in-doubt, record committed to ensure idempotency
                committedTransactions.add(txId);
            }
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            log.error("Shard {} failed to commit transaction {}", shardId, txId, e);
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> abort(TransactionId txId) {
        Objects.requireNonNull(txId, "txId cannot be null");

        // Idempotent abort check
        if (abortedTransactions.contains(txId)) {
            return CompletableFuture.completedFuture(null);
        }

        lock.lock();
        try {
            PreparedContext ctx = inDoubtTransactions.remove(txId);
            if (ctx != null) {
                try {
                    ctx.tx().abort();
                    log.info("Shard {} ABORTED transaction {}", shardId, txId);
                } finally {
                    for (String k : ctx.lockedKeys()) {
                        activeKeyLocks.remove(k, txId);
                    }
                    abortedTransactions.add(txId);
                }
            } else {
                abortedTransactions.add(txId);
            }
            return CompletableFuture.completedFuture(null);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ShardId shardId() {
        return shardId;
    }

    @Override
    public Optional<ParticipantVote> getPreparedState(TransactionId txId) {
        if (inDoubtTransactions.containsKey(txId)) {
            return Optional.of(ParticipantVote.PREPARED);
        }
        if (abortedTransactions.contains(txId)) {
            return Optional.of(ParticipantVote.ABORT);
        }
        if (committedTransactions.contains(txId)) {
            return Optional.of(ParticipantVote.PREPARED);
        }
        return Optional.empty();
    }

    public TransactionManager transactionManager() {
        return transactionManager;
    }

    public boolean isKeyLocked(String key) {
        return activeKeyLocks.containsKey(key);
    }

    public int inDoubtCount() {
        return inDoubtTransactions.size();
    }
}
