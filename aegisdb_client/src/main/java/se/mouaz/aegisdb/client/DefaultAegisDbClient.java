package se.mouaz.aegisdb.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.NotLeaderException;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.statemachine.KvCommand;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Standard implementation of AegisDbClient supporting automatic leader discovery,
 * transparent redirects on NotLeaderException, and exponential backoff retry (Sprint 5; AC4, Milestone M2).
 */
public class DefaultAegisDbClient implements AegisDbClient {
    private static final Logger log = LoggerFactory.getLogger(DefaultAegisDbClient.class);

    @FunctionalInterface
    public interface NodeInvoker {
        CompletableFuture<byte[]> invoke(NodeId targetNode, byte[] command);
    }

    private final List<NodeId> clusterNodes;
    private final NodeInvoker nodeInvoker;
    private final int maxRetries;
    private final Duration initialBackoff;
    private final Duration maxBackoff;
    private final ScheduledExecutorService retryScheduler;
    private final boolean ownsScheduler;

    private volatile NodeId currentLeader;
    private final AtomicInteger roundRobinIndex = new AtomicInteger(0);

    public DefaultAegisDbClient(
            List<NodeId> clusterNodes,
            NodeInvoker nodeInvoker,
            int maxRetries,
            Duration initialBackoff,
            Duration maxBackoff,
            ScheduledExecutorService retryScheduler) {
        if (clusterNodes == null || clusterNodes.isEmpty()) {
            throw new IllegalArgumentException("clusterNodes cannot be empty");
        }
        this.clusterNodes = new ArrayList<>(clusterNodes);
        this.nodeInvoker = Objects.requireNonNull(nodeInvoker, "nodeInvoker cannot be null");
        this.maxRetries = Math.max(1, maxRetries);
        this.initialBackoff = initialBackoff != null ? initialBackoff : Duration.ofMillis(50);
        this.maxBackoff = maxBackoff != null ? maxBackoff : Duration.ofSeconds(2);
        this.ownsScheduler = retryScheduler == null;
        this.retryScheduler = retryScheduler != null ? retryScheduler : Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "aegisdb-client-retry");
            t.setDaemon(true);
            return t;
        });
        this.currentLeader = this.clusterNodes.get(0);
    }

    public DefaultAegisDbClient(List<NodeId> clusterNodes, NodeInvoker nodeInvoker) {
        this(clusterNodes, nodeInvoker, 15, Duration.ofMillis(40), Duration.ofSeconds(1), null);
    }

    public static DefaultAegisDbClient forNodes(Map<NodeId, RaftNode> clusterNodes) {
        Objects.requireNonNull(clusterNodes, "clusterNodes cannot be null");
        return new DefaultAegisDbClient(
                new ArrayList<>(clusterNodes.keySet()),
                (target, cmd) -> {
                    RaftNode node = clusterNodes.get(target);
                    if (node == null) {
                        return CompletableFuture.failedFuture(new IllegalArgumentException("Node " + target + " not found"));
                    }
                    return node.executeClientCommand(cmd);
                }
        );
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        KvCommand command = KvCommand.put(key, value);
        return executeWithRetry(command.toBytes(), 0)
                .thenApply(res -> null);
    }

    @Override
    public CompletableFuture<Optional<byte[]>> get(String key) {
        KvCommand command = KvCommand.get(key);
        return executeWithRetry(command.toBytes(), 0)
                .thenApply(bytes -> (bytes == null || bytes.length == 0) ? Optional.empty() : Optional.of(bytes));
    }

    @Override
    public CompletableFuture<Optional<byte[]>> delete(String key) {
        KvCommand command = KvCommand.delete(key);
        return executeWithRetry(command.toBytes(), 0)
                .thenApply(bytes -> (bytes == null || bytes.length == 0) ? Optional.empty() : Optional.of(bytes));
    }

    @Override
    public Optional<NodeId> currentLeader() {
        return Optional.ofNullable(currentLeader);
    }

    private CompletableFuture<byte[]> executeWithRetry(byte[] command, int attempt) {
        NodeId target = selectTargetNode();
        log.debug("Dispatching client command to node {} (attempt {})", target, attempt);

        CompletableFuture<byte[]> resultFuture = new CompletableFuture<>();

        nodeInvoker.invoke(target, command).whenComplete((response, throwable) -> {
            if (throwable == null) {
                // Success: update tracked leader
                this.currentLeader = target;
                resultFuture.complete(response);
            } else {
                Throwable cause = unwrap(throwable);
                log.debug("Command to {} failed on attempt {}: {}", target, attempt, cause.getMessage());

                if (attempt >= maxRetries) {
                    log.warn("Exceeded maximum retries ({}) for client command", maxRetries, cause);
                    resultFuture.completeExceptionally(cause);
                    return;
                }

                // Handle leader discovery / redirect
                if (cause instanceof NotLeaderException notLeader) {
                    if (notLeader.currentLeader().isPresent()) {
                        NodeId redirected = notLeader.currentLeader().get();
                        log.debug("Learned new leader {} from redirect", redirected);
                        this.currentLeader = redirected;
                    } else {
                        // Leader unknown, advance to next cluster node
                        advanceLeader();
                    }
                } else {
                    // Communication error / node crash: rotate away from target
                    advanceLeader();
                }

                // Exponential backoff
                long backoffMs = Math.min(maxBackoff.toMillis(), (long) (initialBackoff.toMillis() * Math.pow(1.5, attempt)));
                retryScheduler.schedule(() -> {
                    executeWithRetry(command, attempt + 1)
                            .whenComplete((r, ex) -> {
                                if (ex != null) {
                                    resultFuture.completeExceptionally(ex);
                                } else {
                                    resultFuture.complete(r);
                                }
                            });
                }, backoffMs, TimeUnit.MILLISECONDS);
            }
        });

        return resultFuture;
    }

    private synchronized NodeId selectTargetNode() {
        if (currentLeader != null && clusterNodes.contains(currentLeader)) {
            return currentLeader;
        }
        int idx = Math.abs(roundRobinIndex.getAndIncrement() % clusterNodes.size());
        return clusterNodes.get(idx);
    }

    private synchronized void advanceLeader() {
        int idx = Math.abs(roundRobinIndex.getAndIncrement() % clusterNodes.size());
        this.currentLeader = clusterNodes.get(idx);
        log.debug("Advanced candidate leader to {}", currentLeader);
    }

    private Throwable unwrap(Throwable ex) {
        if (ex instanceof CompletionException || ex instanceof ExecutionException) {
            return ex.getCause() != null ? unwrap(ex.getCause()) : ex;
        }
        return ex;
    }

    @Override
    public synchronized void close() {
        if (ownsScheduler) {
            retryScheduler.shutdownNow();
        }
    }
}
