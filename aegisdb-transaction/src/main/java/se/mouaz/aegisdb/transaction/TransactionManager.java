package se.mouaz.aegisdb.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClientId;
import se.mouaz.aegisdb.common.RequestId;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.mvcc.Snapshot;
import se.mouaz.aegisdb.mvcc.VersionChain;
import se.mouaz.aegisdb.mvcc.VersionedValue;
import se.mouaz.aegisdb.transaction.log.InMemoryTransactionLog;
import se.mouaz.aegisdb.transaction.log.TransactionLog;
import se.mouaz.aegisdb.transaction.log.TransactionLogEntry;

import java.io.Closeable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

/**
 * Central transaction manager coordinating atomic single-shard transactions, isolation guarantees,
 * concurrency conflict validation, and durable logging (Master Project Plan §5, §9, §11, §12, §15, §18; US012).
 */
public class TransactionManager implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(TransactionManager.class);

    private final MvccStore mvccStore;
    private final TransactionLog transactionLog;
    private final TransactionRegistry registry;
    private final CommitValidator commitValidator;
    private final TransactionConfig config;
    private final TransactionMetrics metrics;

    private final ConcurrentMap<String, Long> writeLocks = new ConcurrentHashMap<>();
    private final Object globalCommitLock = new Object();

    public TransactionManager(MvccStore mvccStore) {
        this(mvccStore, new InMemoryTransactionLog(), TransactionConfig.defaultConfig());
    }

    public TransactionManager(MvccStore mvccStore, TransactionLog transactionLog) {
        this(mvccStore, transactionLog, TransactionConfig.defaultConfig());
    }

    public TransactionManager(MvccStore mvccStore, TransactionLog transactionLog, TransactionConfig config) {
        this.mvccStore = Objects.requireNonNull(mvccStore, "mvccStore must not be null");
        this.transactionLog = Objects.requireNonNull(transactionLog, "transactionLog must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.registry = new TransactionRegistry();
        this.commitValidator = new CommitValidator(config);
        this.metrics = new TransactionMetrics();
    }

    // ==========================================
    // Transaction Lifecycle Management
    // ==========================================

    public Transaction beginTransaction() {
        return beginTransaction(config.defaultIsolationLevel(), null, null);
    }

    public Transaction beginTransaction(IsolationLevel level) {
        return beginTransaction(level, null, null);
    }

    public Transaction beginTransaction(IsolationLevel level, ClientId clientId, RequestId requestId) {
        Objects.requireNonNull(level, "level must not be null");

        // Idempotency check for existing client request
        if (clientId != null && requestId != null) {
            Optional<TransactionId> existing = registry.findByClientRequest(clientId, requestId);
            if (existing.isPresent()) {
                TransactionId txId = existing.get();
                Optional<TransactionContext> existingCtx = registry.get(txId);
                if (existingCtx.isPresent()) {
                    return new TransactionImpl(existingCtx.get(), this, config);
                }
                if (registry.isCommitted(txId)) {
                    throw new IllegalStateException("Transaction for client request " + clientId.value() + "::" + requestId.sequenceNumber() + " already committed (txId=" + txId + ")");
                }
                if (registry.isAborted(txId)) {
                    throw new IllegalStateException("Transaction for client request " + clientId.value() + "::" + requestId.sequenceNumber() + " was already aborted (txId=" + txId + ")");
                }
            }
        }

        long internalTxId = mvccStore.beginTransaction();
        Snapshot snapshot = mvccStore.createSnapshotForTransaction(internalTxId);

        TransactionId txId = TransactionId.of(internalTxId);
        TransactionContext context = new TransactionContext(
                txId,
                level,
                snapshot.readTimestamp(),
                snapshot,
                clientId,
                requestId
        );

        registry.register(context);
        transactionLog.logBegin(txId, context.startTimestamp());
        metrics.incrementStarted();

        log.debug("Transaction {} started (isolation={}, startTs={})", txId, level, context.startTimestamp());
        return new TransactionImpl(context, this, config);
    }

    public Optional<byte[]> readUnderTransaction(TransactionContext context, String key) {
        checkTimeout(context);

        Snapshot snapshot = context.snapshot();
        Optional<byte[]> value = mvccStore.get(key, snapshot);

        // Record read version timestamp in ReadSet for serializability validation
        VersionChain chain = mvccStore.chains().get(key);
        if (chain != null) {
            Optional<VersionedValue> nodeOpt = chain.findVisibleNode(snapshot);
            long observedTs = nodeOpt.map(VersionedValue::commitTimestamp).orElse(0L);
            context.readSet().recordRead(key, observedTs);
        } else {
            context.readSet().recordRead(key, 0L);
        }

        return value;
    }

    public boolean acquireWriteLock(TransactionContext context, String key) {
        long txId = context.id().value();
        Long existingLock = writeLocks.putIfAbsent(key, txId);
        boolean newlyAcquired = (existingLock == null);
        if (existingLock != null && existingLock != txId) {
            metrics.incrementWriteConflict();
            throw new WriteConflictException(key, txId, existingLock);
        }

        // Eager First-Committer-Wins: check if a newer version committed since this tx started
        VersionChain chain = mvccStore.chains().get(key);
        if (chain != null) {
            VersionedValue node = chain.head();
            while (node != null && !node.isCommitted()) {
                node = node.next();
            }
            if (node != null && node.createTxId() != txId) {
                boolean committedAfterStart = node.commitTimestamp() > context.startTimestamp();
                boolean committedByInFlight = context.snapshot().activeTxIds().contains(node.createTxId());
                if (committedAfterStart || committedByInFlight) {
                    writeLocks.remove(key, txId);
                    metrics.incrementWriteConflict();
                    throw new WriteConflictException(key, txId, node.createTxId());
                }
            }
        }
        return newlyAcquired;
    }

    public void releaseWriteLock(TransactionContext context, String key) {
        writeLocks.remove(key, context.id().value());
    }

    public void prepare(TransactionId txId) {
        Objects.requireNonNull(txId, "txId must not be null");

        TransactionContext context = getRequiredContext(txId);
        if (context.state() == TransactionState.PREPARED) {
            return; // idempotent
        }

        synchronized (globalCommitLock) {
            checkTimeout(context);
            context.transitionTo(TransactionState.PREPARING);

            try {
                commitValidator.validate(context, mvccStore);
            } catch (WriteConflictException e) {
                doAbort(context);
                throw e;
            } catch (SerializationFailureException e) {
                doAbort(context);
                throw e;
            }

            context.transitionTo(TransactionState.PREPARED);
            transactionLog.logPrepare(txId, System.currentTimeMillis(), context.writeSet());
            log.debug("Transaction {} PREPARED successfully", txId);
        }
    }

    public long commit(TransactionId txId) {
        Objects.requireNonNull(txId, "txId must not be null");

        // Idempotency: if already committed, return recorded commit timestamp
        if (registry.isCommitted(txId)) {
            return registry.getCommitTimestamp(txId).orElse(0L);
        }
        if (registry.isAborted(txId)) {
            throw new IllegalStateException("Cannot commit aborted transaction: " + txId);
        }

        TransactionContext context = getRequiredContext(txId);

        synchronized (globalCommitLock) {
            checkTimeout(context);

            if (context.state() == TransactionState.ACTIVE) {
                context.transitionTo(TransactionState.PREPARING);
                try {
                    commitValidator.validate(context, mvccStore);
                } catch (WriteConflictException e) {
                    doAbort(context);
                    throw e;
                } catch (SerializationFailureException e) {
                    doAbort(context);
                    throw e;
                }
            }

            // Durability point of no return: persist redo information and decision
            context.transitionTo(TransactionState.COMMIT_DECIDED);
            long logTimestamp = System.currentTimeMillis();
            transactionLog.logCommitDecided(txId, logTimestamp, context.writeSet());
            
            context.transitionTo(TransactionState.COMMITTING);

            // Apply write set mutations directly into MVCC store
            long internalId = txId.value();
            for (WriteOperation op : context.writeSet().operations().values()) {
                if (op.type() == OperationType.PUT) {
                    mvccStore.put(op.key(), op.value(), internalId);
                } else if (op.type() == OperationType.DELETE) {
                    mvccStore.delete(op.key(), internalId);
                }
            }

            // Commit atomic version changes in MVCC store
            long commitTimestamp = mvccStore.commit(internalId);
            context.setCommitTimestamp(commitTimestamp);
            context.transitionTo(TransactionState.COMMITTED);

            // Release write locks
            for (String key : context.writeSet().keys()) {
                writeLocks.remove(key, txId.value());
            }

            // Close snapshot and register outcome
            context.snapshot().close();
            registry.markCommitted(txId, commitTimestamp);
            metrics.incrementCommitted();

            log.debug("Transaction {} COMMITTED at timestamp {}", txId, commitTimestamp);
            return commitTimestamp;
        }
    }

    public void abort(TransactionId txId) {
        Objects.requireNonNull(txId, "txId must not be null");

        if (registry.isAborted(txId)) {
            return; // idempotent
        }
        if (registry.isCommitted(txId)) {
            throw new IllegalStateException("Cannot abort committed transaction: " + txId);
        }

        Optional<TransactionContext> ctxOpt = registry.get(txId);
        if (ctxOpt.isEmpty()) {
            registry.markAborted(txId);
            return;
        }

        TransactionContext context = ctxOpt.get();
        synchronized (globalCommitLock) {
            doAbort(context);
        }
    }

    private void doAbort(TransactionContext context) {
        try {
            context.transitionTo(TransactionState.ABORTED);
        } catch (IllegalStateException ignored) {
            // already aborted or terminal
        }

        TransactionId txId = context.id();
        
        // Roll back any uncommitted versions in MVCC store
        mvccStore.abort(txId.value());

        // Release write locks
        for (String key : context.writeSet().keys()) {
            writeLocks.remove(key, txId.value());
        }

        context.snapshot().close();
        transactionLog.logAbort(txId, System.currentTimeMillis());
        registry.markAborted(txId);
        metrics.incrementAborted();

        log.debug("Transaction {} ABORTED and cleaned up", txId);
    }

    // ==========================================
    // Automatic Retries & Functional Wrapper
    // ==========================================

    public <T> T runInTransaction(Function<Transaction, T> action) {
        return runInTransaction(config.defaultIsolationLevel(), action, config.defaultMaxRetries());
    }

    public <T> T runInTransaction(IsolationLevel level, Function<Transaction, T> action, int maxRetries) {
        Objects.requireNonNull(level, "level must not be null");
        Objects.requireNonNull(action, "action must not be null");

        int attempts = 0;
        while (true) {
            attempts++;
            Transaction tx = beginTransaction(level);
            try {
                T result = action.apply(tx);
                if (!tx.state().isTerminal()) {
                    tx.commit();
                }
                return result;
            } catch (WriteConflictException | SerializationFailureException e) {
                tx.abort();
                if (attempts > maxRetries) {
                    throw e;
                }
                backoff(attempts);
            } catch (RuntimeException e) {
                tx.abort();
                throw e;
            }
        }
    }

    private void backoff(int attempt) {
        try {
            long sleepMs = (long) (ThreadLocalRandom.current().nextInt(5, 15) * Math.pow(1.5, Math.min(attempt, 5)));
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransactionException("Transaction retry backoff interrupted", e);
        }
    }

    // ==========================================
    // Crash Recovery & Sweep
    // ==========================================

    public void recoverFromLog() {
        synchronized (globalCommitLock) {
            List<TransactionLogEntry> entries = transactionLog.replay();
            for (TransactionLogEntry entry : entries) {
                switch (entry.state()) {
                    case COMMIT_DECIDED, COMMITTED -> {
                        // Apply writes from redo log if they haven't been applied
                        if (entry.writeOperations() != null && !entry.writeOperations().isEmpty()) {
                            long internalId = mvccStore.beginTransaction();
                            for (WriteOperation op : entry.writeOperations()) {
                                if (op.type() == OperationType.PUT) {
                                    mvccStore.put(op.key(), op.value(), internalId);
                                } else if (op.type() == OperationType.DELETE) {
                                    mvccStore.delete(op.key(), internalId);
                                }
                            }
                            mvccStore.commit(internalId);
                        }
                        registry.markCommitted(entry.txId(), entry.timestamp());
                    }
                    case ABORTED -> registry.markAborted(entry.txId());
                    case PREPARING -> {
                        // Recover prepared transaction for 2PC resolution
                        TransactionContext ctx = new TransactionContext(
                                entry.txId(),
                                config.defaultIsolationLevel(),
                                entry.timestamp(),
                                se.mouaz.aegisdb.mvcc.Snapshot.of(entry.timestamp()),
                                null,
                                null
                        );
                        if (entry.writeOperations() != null) {
                            for (WriteOperation op : entry.writeOperations()) {
                                if (op.type() == OperationType.PUT) {
                                    ctx.writeSet().put(op.key(), op.value(), config.maxWriteSetSize());
                                } else if (op.type() == OperationType.DELETE) {
                                    ctx.writeSet().delete(op.key(), config.maxWriteSetSize());
                                }
                            }
                        }
                        ctx.transitionTo(TransactionState.PREPARING); // to match log state
                        registry.register(ctx);
                        log.info("Recovered PREPARING transaction {} from log", entry.txId());
                    }
                    default -> {
                        // In-flight active transactions at crash time default to aborted
                        log.debug("Log entry {} recovered with state {}", entry.txId(), entry.state());
                    }
                }
            }
            log.info("Recovered transaction manager state from {} log entries", entries.size());
        }
    }

    public int sweepExpiredTransactions() {
        List<TransactionContext> expired = registry.sweepExpired(config.transactionTtl().toMillis());
        for (TransactionContext ctx : expired) {
            log.warn("Transaction {} timed out after {} ms, aborting", ctx.id(),
                    System.currentTimeMillis() - ctx.createdWallClockMillis());
            metrics.incrementTimeout();
            abort(ctx.id());
        }
        return expired.size();
    }

    public void checkTimeout(TransactionContext context) {
        long now = System.currentTimeMillis();
        long ttlMillis = config.transactionTtl().toMillis();
        if (context.isExpired(now, ttlMillis)) {
            metrics.incrementTimeout();
            abort(context.id());
            throw new TransactionTimeoutException(
                    context.id().value(),
                    now - context.createdWallClockMillis(),
                    ttlMillis
            );
        }
    }

    private TransactionContext getRequiredContext(TransactionId txId) {
        return registry.get(txId).orElseThrow(() ->
                new TransactionException("Transaction " + txId + " not found or already finished"));
    }

    public MvccStore mvccStore() {
        return mvccStore;
    }

    public TransactionRegistry registry() {
        return registry;
    }

    public TransactionConfig config() {
        return config;
    }

    public TransactionMetrics metrics() {
        return metrics;
    }

    public TransactionLog transactionLog() {
        return transactionLog;
    }

    @Override
    public void close() {
        sweepExpiredTransactions();
        for (TransactionContext ctx : registry.listActive()) {
            abort(ctx.id());
        }
        transactionLog.close();
    }
}
