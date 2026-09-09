package se.mouaz.aegisdb.client;

import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.transaction.IsolationLevel;
import se.mouaz.aegisdb.transaction.Transaction;
import se.mouaz.aegisdb.transaction.TransactionManager;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Transactional AegisDbClient adapter backed by a local TransactionManager (Sprint 7, US012).
 * Enables developers and integration tests to execute multi-operation transactions and atomic retries
 * through the unified AegisDbClient SDK.
 */
public class LocalTransactionalClient implements AegisDbClient {
    private final TransactionManager transactionManager;
    private final NodeId localNodeId;

    public LocalTransactionalClient(TransactionManager transactionManager) {
        this(transactionManager, NodeId.of("local-shard-leader"));
    }

    public LocalTransactionalClient(TransactionManager transactionManager, NodeId localNodeId) {
        this.transactionManager = Objects.requireNonNull(transactionManager, "transactionManager must not be null");
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId must not be null");
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return CompletableFuture.runAsync(() ->
                transactionManager.runInTransaction(tx -> {
                    tx.put(key, value);
                    return null;
                })
        );
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(String key) {
        return CompletableFuture.supplyAsync(() ->
                transactionManager.runInTransaction(tx -> tx.get(key))
        );
    }

    @Override
    public CompletableFuture<Optional<byte[]>> delete(String key) {
        return CompletableFuture.supplyAsync(() ->
                transactionManager.runInTransaction(tx -> {
                    Optional<byte[]> previous = tx.get(key);
                    tx.delete(key);
                    return previous;
                })
        );
    }

    @Override
    public Transaction beginTransaction(IsolationLevel level) {
        return transactionManager.beginTransaction(level);
    }

    @Override
    public <T> T runInTransaction(IsolationLevel level, Function<Transaction, T> action, int maxRetries) {
        return transactionManager.runInTransaction(level, action, maxRetries);
    }

    @Override
    public Optional<NodeId> currentLeader() {
        return Optional.of(localNodeId);
    }

    public TransactionManager transactionManager() {
        return transactionManager;
    }

    @Override
    public void close() {
        transactionManager.close();
    }
}
