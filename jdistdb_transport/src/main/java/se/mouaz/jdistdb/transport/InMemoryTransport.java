package se.mouaz.jdistdb.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.jdistdb.common.NodeId;
import se.mouaz.jdistdb.protocol.AppendEntriesRequest;
import se.mouaz.jdistdb.protocol.AppendEntriesResponse;
import se.mouaz.jdistdb.protocol.RequestVoteRequest;
import se.mouaz.jdistdb.protocol.RequestVoteResponse;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class InMemoryTransport implements RaftTransport {
    private static final Logger log = LoggerFactory.getLogger(InMemoryTransport.class);

    private static final Map<NodeId, InMemoryTransport> REGISTRY = new ConcurrentHashMap<>();
    private static final Set<NodeId> DISCONNECTED_NODES = ConcurrentHashMap.newKeySet();
    private static final Map<NodeId, Set<NodeId>> PARTITIONS = new ConcurrentHashMap<>();

    private final NodeId nodeId;
    private final Duration requestTimeout;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean dropMessages = new AtomicBoolean(false);
    private volatile Duration artificialDelay = Duration.ZERO;
    private volatile RaftRequestHandler handler;

    public InMemoryTransport(NodeId nodeId, Duration requestTimeout) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.requestTimeout = requestTimeout != null ? requestTimeout : Duration.ofSeconds(2);
    }

    public InMemoryTransport(NodeId nodeId) {
        this(nodeId, Duration.ofSeconds(2));
    }

    @Override
    public NodeId localNodeId() {
        return nodeId;
    }

    @Override
    public void registerHandler(RaftRequestHandler handler) {
        this.handler = Objects.requireNonNull(handler, "handler cannot be null");
    }

    @Override
    public void start() {
        running.set(true);
        REGISTRY.put(nodeId, this);
        log.info("InMemoryTransport started for node {}", nodeId);
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            REGISTRY.remove(nodeId);
            DISCONNECTED_NODES.remove(nodeId);
            PARTITIONS.remove(nodeId);
            log.info("InMemoryTransport stopped for node {}", nodeId);
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(NodeId destination, RequestVoteRequest request) {
        return executeRpc(destination, "RequestVote", () -> {
            InMemoryTransport target = REGISTRY.get(destination);
            if (target == null || !target.isRunning() || target.handler == null) {
                throw TransportException.nodeNotFound("Target node " + destination + " is unreachable or stopped");
            }
            return target.handler.handleRequestVote(request);
        });
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(NodeId destination, AppendEntriesRequest request) {
        return executeRpc(destination, "AppendEntries", () -> {
            InMemoryTransport target = REGISTRY.get(destination);
            if (target == null || !target.isRunning() || target.handler == null) {
                throw TransportException.nodeNotFound("Target node " + destination + " is unreachable or stopped");
            }
            return target.handler.handleAppendEntries(request);
        });
    }

    private <T> CompletableFuture<T> executeRpc(NodeId destination, String rpcName, Callable<CompletableFuture<T>> call) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(
                    TransportException.nodeStopped("Node " + nodeId + " transport is stopped"));
        }

        if (dropMessages.get() || isPartitioned(nodeId, destination) || DISCONNECTED_NODES.contains(nodeId) || DISCONNECTED_NODES.contains(destination)) {
            CompletableFuture<T> droppedFuture = new CompletableFuture<>();
            return applyTimeout(droppedFuture, requestTimeout, rpcName, destination);
        }

        CompletableFuture<T> resultFuture = new CompletableFuture<>();
        Duration delay = this.artificialDelay;

        Runnable task = () -> {
            try {
                InMemoryTransport target = REGISTRY.get(destination);
                if (target == null || !target.isRunning() || target.handler == null) {
                    resultFuture.completeExceptionally(
                            TransportException.nodeNotFound("Target node " + destination + " is unreachable or stopped"));
                    return;
                }
                if (target.dropMessages.get()) {
                    // Receiver drops incoming messages -> do not complete, let caller time out
                    return;
                }

                CompletableFuture<T> internalFuture = call.call();
                internalFuture.whenComplete((resp, ex) -> {
                    if (ex != null) {
                        Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
                        resultFuture.completeExceptionally(
                                cause instanceof TransportException ? cause : TransportException.networkError("RPC " + rpcName + " failed", cause));
                    } else {
                        resultFuture.complete(resp);
                    }
                });
            } catch (Exception ex) {
                resultFuture.completeExceptionally(
                        ex instanceof TransportException ? ex : TransportException.networkError("RPC invocation error", ex));
            }
        };

        if (delay.isZero() || delay.isNegative()) {
            CompletableFuture.runAsync(task);
        } else {
            CompletableFuture.runAsync(task, CompletableFuture.delayedExecutor(delay.toMillis(), TimeUnit.MILLISECONDS));
        }

        return applyTimeout(resultFuture, requestTimeout, rpcName, destination);
    }

    private <T> CompletableFuture<T> applyTimeout(CompletableFuture<T> future, Duration timeout, String rpcName, NodeId destination) {
        return future.orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
                .exceptionally(ex -> {
                    Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
                    if (cause instanceof TimeoutException) {
                        throw TransportException.timeout("RPC " + rpcName + " to " + destination + " timed out after " + timeout.toMillis() + " ms");
                    }
                    if (cause instanceof RuntimeException re) throw re;
                    throw new CompletionException(cause);
                });
    }

    // --- Fault Injection Helpers ---

    public void setDropMessages(boolean drop) {
        this.dropMessages.set(drop);
    }

    public void setArtificialDelay(Duration delay) {
        this.artificialDelay = delay != null ? delay : Duration.ZERO;
    }

    public static void disconnectNode(NodeId node) {
        DISCONNECTED_NODES.add(node);
    }

    public static void reconnectNode(NodeId node) {
        DISCONNECTED_NODES.remove(node);
    }

    public static void partition(Set<NodeId> groupA, Set<NodeId> groupB) {
        for (NodeId a : groupA) {
            PARTITIONS.computeIfAbsent(a, k -> ConcurrentHashMap.newKeySet()).addAll(groupB);
        }
        for (NodeId b : groupB) {
            PARTITIONS.computeIfAbsent(b, k -> ConcurrentHashMap.newKeySet()).addAll(groupA);
        }
    }

    public static void healPartitions() {
        PARTITIONS.clear();
        DISCONNECTED_NODES.clear();
    }

    public static void clearRegistry() {
        REGISTRY.clear();
        DISCONNECTED_NODES.clear();
        PARTITIONS.clear();
    }

    private static boolean isPartitioned(NodeId from, NodeId to) {
        Set<NodeId> blocked = PARTITIONS.get(from);
        return blocked != null && blocked.contains(to);
    }
}
