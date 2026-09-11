package se.mouaz.aegisdb.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.raft.statemachine.KvCommand;
import se.mouaz.aegisdb.sharding.*;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.transaction.IsolationLevel;
import se.mouaz.aegisdb.transaction.Transaction;
import se.mouaz.aegisdb.transaction.distributed.DistributedTransaction;
import se.mouaz.aegisdb.transaction.distributed.DistributedTransactionCoordinator;
import se.mouaz.aegisdb.transaction.distributed.InMemoryCoordinatorLog;
import se.mouaz.aegisdb.transaction.distributed.TransactionCoordinatorLog;
import se.mouaz.aegisdb.transaction.distributed.TransactionParticipant;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Shard-aware implementation of AegisDbClient (Master Project Plan §5, §10; US013, US014).
 * Routes key-value operations across multiple shards and Raft consensus groups,
 * handles leader tracking and failover, and enforces single-shard transaction boundaries.
 */
public class ShardedAegisDbClient implements AegisDbClient {
    private static final Logger log = LoggerFactory.getLogger(ShardedAegisDbClient.class);

    private final QueryRouter queryRouter;
    private final Map<ShardId, AegisDbClient> shardClients;
    private final DistributedTransactionCoordinator coordinator;

    public ShardedAegisDbClient(QueryRouter queryRouter) {
        this(queryRouter, Collections.emptyMap(), (DistributedTransactionCoordinator) null);
    }

    public ShardedAegisDbClient(QueryRouter queryRouter, Map<ShardId, AegisDbClient> shardClients) {
        this(queryRouter, shardClients, (DistributedTransactionCoordinator) null);
    }

    public ShardedAegisDbClient(
            QueryRouter queryRouter,
            Map<ShardId, AegisDbClient> shardClients,
            DistributedTransactionCoordinator coordinator
    ) {
        this.queryRouter = Objects.requireNonNull(queryRouter, "queryRouter cannot be null");
        this.shardClients = shardClients != null ? new HashMap<>(shardClients) : Collections.emptyMap();
        this.coordinator = coordinator;
    }

    public ShardedAegisDbClient(
            QueryRouter queryRouter,
            Map<ShardId, AegisDbClient> shardClients,
            Map<ShardId, TransactionParticipant> participants
    ) {
        this(queryRouter, shardClients, participants, new InMemoryCoordinatorLog());
    }

    public ShardedAegisDbClient(
            QueryRouter queryRouter,
            Map<ShardId, AegisDbClient> shardClients,
            Map<ShardId, TransactionParticipant> participants,
            TransactionCoordinatorLog coordinatorLog
    ) {
        this.queryRouter = Objects.requireNonNull(queryRouter, "queryRouter cannot be null");
        this.shardClients = shardClients != null ? new HashMap<>(shardClients) : Collections.emptyMap();
        Objects.requireNonNull(participants, "participants cannot be null");
        Objects.requireNonNull(coordinatorLog, "coordinatorLog cannot be null");
        this.coordinator = new DistributedTransactionCoordinator(
                coordinatorLog,
                participants::get
        );
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        KvCommand command = KvCommand.put(key, value);
        return queryRouter.execute(key, command.toBytes())
                .thenApply(res -> null);
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(String key) {
        KvCommand command = KvCommand.get(key);
        return queryRouter.execute(key, command.toBytes())
                .thenApply(bytes -> (bytes == null || bytes.length == 0) ? Optional.empty() : Optional.of(bytes));
    }

    @Override
    public CompletableFuture<Optional<byte[]>> delete(String key) {
        KvCommand command = KvCommand.delete(key);
        return queryRouter.execute(key, command.toBytes())
                .thenApply(bytes -> (bytes == null || bytes.length == 0) ? Optional.empty() : Optional.of(bytes));
    }

    /**
     * Returns the tracked leader for the primary/default shard (shard-0).
     */
    @Override
    public Optional<NodeId> currentLeader() {
        ShardMap map = queryRouter.shardRouter().shardMap();
        if (map.shardCount() > 0) {
            return map.getShardByIndex(0).flatMap(s -> queryRouter.leaderLocator().getLeader(s.id()));
        }
        return Optional.empty();
    }

    /**
     * Returns the active leader for the shard that owns the given key.
     */
    public Optional<NodeId> currentLeader(String key) {
        Shard shard = queryRouter.shardRouter().route(key);
        return queryRouter.leaderLocator().getLeader(shard.id());
    }

    /**
     * Returns the active leader for a specific shard.
     */
    public Optional<NodeId> currentLeader(ShardId shardId) {
        return queryRouter.leaderLocator().getLeader(shardId);
    }

    /**
     * Resolves the target ShardId for a given key.
     */
    public ShardId resolveShard(String key) {
        return queryRouter.shardRouter().routeToShardId(key);
    }

    /**
     * Starts a single-shard transaction targeting a specific shard.
     */
    public Transaction beginTransaction(ShardId shardId, IsolationLevel level) {
        AegisDbClient client = shardClients.get(shardId);
        if (client == null) {
            throw new UnsupportedOperationException("No transactional client registered for shard: " + shardId);
        }
        return client.beginTransaction(level);
    }

    private final java.util.concurrent.atomic.AtomicLong txCounter =
            new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis() * 1000L);

    @Override
    public Transaction beginTransaction(IsolationLevel level) {
        ShardMap map = queryRouter.shardRouter().shardMap();
        if (map.shardCount() == 1 && coordinator == null) {
            return beginTransaction(map.getShardByIndex(0).get().id(), level);
        }
        if (coordinator != null) {
            TransactionId txId = TransactionId.of(txCounter.getAndIncrement());
            return new DistributedTransaction(
                    txId,
                    level,
                    key -> queryRouter.shardRouter().routeToShardId(key),
                    (shardId, key) -> {
                        try {
                            return get(key).join();
                        } catch (Exception e) {
                            log.error("Failed to read key {} from shard {}", key, shardId, e);
                            return Optional.empty();
                        }
                    },
                    coordinator
            );
        }
        throw new UnsupportedOperationException(
                "Multi-shard cluster requires explicit shard targeting: beginTransaction(ShardId, IsolationLevel) " +
                "or configured DistributedTransactionCoordinator for Two-Phase Commit.");
    }

    @Override
    public <T> T runInTransaction(IsolationLevel level, Function<Transaction, T> action, int maxRetries) {
        ShardMap map = queryRouter.shardRouter().shardMap();
        if (map.shardCount() == 1 && coordinator == null) {
            ShardId singleShard = map.getShardByIndex(0).get().id();
            AegisDbClient client = shardClients.get(singleShard);
            if (client != null) {
                return client.runInTransaction(level, action, maxRetries);
            }
        }
        if (coordinator != null) {
            int attempts = 0;
            while (true) {
                attempts++;
                Transaction tx = beginTransaction(level);
                try {
                    T result = action.apply(tx);
                    tx.commit();
                    return result;
                } catch (Exception e) {
                    tx.abort();
                    if (attempts >= maxRetries) {
                        throw e instanceof RuntimeException re ? re : new RuntimeException(e);
                    }
                    try {
                        long sleepMs = java.util.concurrent.ThreadLocalRandom.current().nextLong(5, 20) + (5L * Math.min(attempts, 10));
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during transaction retry", ie);
                    }
                }
            }
        }
        throw new UnsupportedOperationException(
                "Multi-shard transactions require explicit shard targeting or configured DistributedTransactionCoordinator.");
    }

    public QueryRouter queryRouter() {
        return queryRouter;
    }

    public ShardRouter shardRouter() {
        return queryRouter.shardRouter();
    }

    public ShardMap shardMap() {
        return queryRouter.shardRouter().shardMap();
    }

    public DistributedTransactionCoordinator coordinator() {
        return coordinator;
    }

    @Override
    public void close() {
        queryRouter.close();
        if (coordinator != null) {
            try {
                coordinator.close();
            } catch (Exception e) {
                log.warn("Error closing 2PC coordinator", e);
            }
        }
        for (AegisDbClient client : shardClients.values()) {
            try {
                client.close();
            } catch (Exception e) {
                log.warn("Error closing shard client", e);
            }
        }
    }
}
