package se.mouaz.aegisdb.transaction;

import se.mouaz.aegisdb.common.TransactionId;

import java.util.Objects;
import java.util.Optional;

/**
 * Concrete transaction handle executing operations within an active transaction context
 * (Master Project Plan §9 & §18).
 */
public class TransactionImpl implements Transaction {
    private final TransactionContext context;
    private final TransactionManager manager;
    private final TransactionConfig config;

    public TransactionImpl(TransactionContext context, TransactionManager manager, TransactionConfig config) {
        this.context = Objects.requireNonNull(context, "context must not be null");
        this.manager = Objects.requireNonNull(manager, "manager must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    @Override
    public TransactionId id() {
        return context.id();
    }

    @Override
    public TransactionState state() {
        return context.state();
    }

    @Override
    public IsolationLevel isolationLevel() {
        return context.isolationLevel();
    }

    @Override
    public Optional<byte[]> get(String key) {
        ensureActive();
        Objects.requireNonNull(key, "key must not be null");

        // 1. Read-Your-Own-Writes from WriteSet
        Optional<WriteOperation> localOp = context.writeSet().get(key);
        if (localOp.isPresent()) {
            WriteOperation op = localOp.get();
            if (op.type() == OperationType.DELETE) {
                return Optional.empty();
            }
            return Optional.of(op.value());
        }

        // 2. Read from MVCC store under snapshot
        return manager.readUnderTransaction(context, key);
    }

    @Override
    public void put(String key, byte[] value) {
        ensureActive();
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");

        manager.checkTimeout(context);
        manager.acquireWriteLock(context, key);
        context.writeSet().put(key, value, config.maxWriteSetSize());
    }

    @Override
    public void delete(String key) {
        ensureActive();
        Objects.requireNonNull(key, "key must not be null");

        manager.checkTimeout(context);
        manager.acquireWriteLock(context, key);
        context.writeSet().delete(key, config.maxWriteSetSize());
    }

    @Override
    public void prepare() {
        manager.prepare(context.id());
    }

    @Override
    public long commit() {
        return manager.commit(context.id());
    }

    @Override
    public void abort() {
        manager.abort(context.id());
    }

    public TransactionContext context() {
        return context;
    }

    private void ensureActive() {
        TransactionState current = context.state();
        if (current != TransactionState.ACTIVE) {
            throw new IllegalStateException("Transaction " + context.id() + " is not ACTIVE, current state: " + current);
        }
    }
}
