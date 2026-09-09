package se.mouaz.aegisdb.sharding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.raft.NotLeaderException;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * High-performance query router coordinating transparent key-to-shard dispatching,
 * dynamic leader locating, transparent redirect caching, and exponential backoff retries (Master Project Plan §10; US014).
 */
public class QueryRouter implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(QueryRouter.class);

    @FunctionalInterface
    public interface ShardNodeInvoker {
        CompletableFuture<byte[]> invoke(ShardId shardId, NodeId targetNode, byte[] command);
    }

    private final ShardRouter shardRouter;
    private final LeaderLocator leaderLocator;
    private final ShardNodeInvoker invoker;
    private final int maxRetries;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final ScheduledExecutorService scheduler;
    private final boolean ownsScheduler;

    private final Map<ShardId, AtomicInteger> roundRobinIndices = new ConcurrentHashMap<>();

    public QueryRouter(
            ShardRouter shardRouter,
            LeaderLocator leaderLocator,
            ShardNodeInvoker invoker,
            int maxRetries,
            Duration initialBackoff,
            Duration maxBackoff,
            ScheduledExecutorService scheduler) {
        this.shardRouter = Objects.requireNonNull(shardRouter, "shardRouter cannot be null");
        this.leaderLocator = Objects.requireNonNull(leaderLocator, "leaderLocator cannot be null");
        this.invoker = Objects.requireNonNull(invoker, "invoker cannot be null");
        this.maxRetries = Math.max(1, maxRetries);
        this.initialBackoff = initialBackoff != null ? initialBackoff : Duration.ofMillis(30);
        this.maxBackoff = maxBackoff != null ? maxBackoff : Duration.ofSeconds(1);
        this.ownsScheduler = scheduler == null;
        this.scheduler = scheduler != null ? scheduler : Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "aegisdb-query-router");
            t.setDaemon(true);
            return t;
        });
    }

    public QueryRouter(ShardRouter shardRouter, LeaderLocator leaderLocator, ShardNodeInvoker invoker) {
        this(shardRouter, leaderLocator, invoker, 12, Duration.ofMillis(30), Duration.ofSeconds(1), null);
    }

    /**
     * Executes a command targeted at a specific key, automatically determining the shard and leader.
     */
    public CompletableFuture<byte[]> execute(String key, byte[] command) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(command, "command cannot be null");

        Shard shard = shardRouter.route(key);
        return executeOnShard(shard.id(), command, 0);
    }

    /**
     * Executes a command on a specific shard with automatic leader resolution and retry.
     */
    public CompletableFuture<byte[]> executeOnShard(ShardId shardId, byte[] command) {
        return executeOnShard(shardId, command, 0);
    }

    private CompletableFuture<byte[]> executeOnShard(ShardId shardId, byte[] command, int attempt) {
        Shard shard = shardRouter.shardMap().getShard(shardId)
                .orElseThrow(() -> new IllegalStateException("Shard " + shardId + " not found in ShardMap"));
        ReplicationGroup replicationGroup = shard.replicationGroup();

        NodeId targetNode = selectTargetNode(shardId, replicationGroup);

        CompletableFuture<byte[]> future = new CompletableFuture<>();

        try {
            invoker.invoke(shardId, targetNode, command).whenComplete((result, throwable) -> {
                if (throwable == null) {
                    // Update known leader upon confirmed successful response
                    leaderLocator.updateLeader(shardId, targetNode);
                    future.complete(result);
                } else {
                    Throwable cause = unwrap(throwable);
                    handleFailure(shardId, targetNode, command, attempt, cause, future);
                }
            });
        } catch (Exception e) {
            handleFailure(shardId, targetNode, command, attempt, e, future);
        }

        return future;
    }

    private void handleFailure(ShardId shardId, NodeId targetNode, byte[] command, int attempt,
                               Throwable cause, CompletableFuture<byte[]> future) {
        if (cause instanceof NotLeaderException notLeader) {
            Optional<NodeId> newLeaderHint = notLeader.currentLeader();
            if (newLeaderHint.isPresent()) {
                log.debug("Received NotLeader from {} with leader hint {} for shard {}",
                        targetNode, newLeaderHint.get(), shardId);
                leaderLocator.updateLeader(shardId, newLeaderHint.get());
            } else {
                log.debug("Received NotLeader from {} without hint for shard {}", targetNode, shardId);
                leaderLocator.invalidateLeader(shardId, targetNode);
            }
        } else {
            log.warn("Execution failed on {} for shard {}: {}", targetNode, shardId, cause.getMessage());
            leaderLocator.invalidateLeader(shardId, targetNode);
        }

        if (attempt >= maxRetries) {
            future.completeExceptionally(cause);
            return;
        }

        long delayMs = Math.min(
                initialBackoff.toMillis() * (1L << Math.min(attempt, 6)),
                maxBackoff.toMillis()
        );

        scheduler.schedule(() -> {
            executeOnShard(shardId, command, attempt + 1).whenComplete((res, err) -> {
                if (err != null) {
                    future.completeExceptionally(err);
                } else {
                    future.complete(res);
                }
            });
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private NodeId selectTargetNode(ShardId shardId, ReplicationGroup group) {
        Optional<NodeId> leader = leaderLocator.getLeader(shardId);
        if (leader.isPresent() && group.contains(leader.get())) {
            return leader.get();
        }

        // Fallback: round-robin across group members
        List<NodeId> members = new ArrayList<>(group.members());
        if (members.isEmpty()) {
            throw new IllegalStateException("Replication group for shard " + shardId + " has no members");
        }
        AtomicInteger rr = roundRobinIndices.computeIfAbsent(shardId, k -> new AtomicInteger(0));
        int index = Math.floorMod(rr.getAndIncrement(), members.size());
        return members.get(index);
    }

    private static Throwable unwrap(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            return t.getCause() != null ? unwrap(t.getCause()) : t;
        }
        return t;
    }

    public ShardRouter shardRouter() {
        return shardRouter;
    }

    public LeaderLocator leaderLocator() {
        return leaderLocator;
    }

    @Override
    public void close() {
        if (ownsScheduler) {
            scheduler.shutdownNow();
        }
    }
}
