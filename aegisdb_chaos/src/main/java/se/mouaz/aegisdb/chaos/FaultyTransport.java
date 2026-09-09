package se.mouaz.aegisdb.chaos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ErrorCode;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.protocol.InstallSnapshotRequest;
import se.mouaz.aegisdb.protocol.InstallSnapshotResponse;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;
import se.mouaz.aegisdb.transport.RaftRequestHandler;
import se.mouaz.aegisdb.transport.RaftTransport;
import se.mouaz.aegisdb.transport.TransportException;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Decorator implementing {@link RaftTransport} that intercepts RPC communications
 * and injects controllable faults (drop, delay, duplicate, partition) per {@link FaultRule}.
 * Supports deterministic pseudo-random seeds for reproducible research experiments (US016).
 */
public class FaultyTransport implements RaftTransport {
    private static final Logger log = LoggerFactory.getLogger(FaultyTransport.class);

    private final RaftTransport delegate;
    private final List<FaultRule> rules = new CopyOnWriteArrayList<>();
    private final Random random;
    private final ScheduledExecutorService delayScheduler;
    private final boolean ownsScheduler;

    // Metrics
    private final AtomicLong droppedMessages = new AtomicLong(0);
    private final AtomicLong delayedMessages = new AtomicLong(0);
    private final AtomicLong duplicatedMessages = new AtomicLong(0);
    private final AtomicLong partitionBlocked = new AtomicLong(0);

    public FaultyTransport(RaftTransport delegate, Long seed, ScheduledExecutorService scheduler) {
        this.delegate = Objects.requireNonNull(delegate, "delegate transport cannot be null");
        this.random = seed != null ? new Random(seed) : new Random();
        if (scheduler != null) {
            this.delayScheduler = scheduler;
            this.ownsScheduler = false;
        } else {
            this.delayScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "faulty-transport-delay-" + delegate.localNodeId());
                t.setDaemon(true);
                return t;
            });
            this.ownsScheduler = true;
        }
    }

    public FaultyTransport(RaftTransport delegate, Long seed) {
        this(delegate, seed, null);
    }

    public FaultyTransport(RaftTransport delegate) {
        this(delegate, null, null);
    }

    public void addRule(FaultRule rule) {
        rules.add(Objects.requireNonNull(rule, "rule cannot be null"));
        log.info("[{}] Injected fault rule: {}", localNodeId(), rule);
    }

    public void removeRule(String ruleId) {
        rules.removeIf(r -> Objects.equals(r.id(), ruleId));
        log.info("[{}] Removed fault rule id: {}", localNodeId(), ruleId);
    }

    public void clearRules() {
        rules.clear();
        log.info("[{}] Cleared all fault rules", localNodeId());
    }

    public List<FaultRule> rules() {
        return List.copyOf(rules);
    }

    public long droppedMessagesCount() {
        return droppedMessages.get();
    }

    public long delayedMessagesCount() {
        return delayedMessages.get();
    }

    public long duplicatedMessagesCount() {
        return duplicatedMessages.get();
    }

    public long partitionBlockedCount() {
        return partitionBlocked.get();
    }

    @Override
    public NodeId localNodeId() {
        return delegate.localNodeId();
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(NodeId destination, RequestVoteRequest request) {
        return dispatchWithFaults(destination, request, () -> delegate.requestVote(destination, request));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(NodeId destination, AppendEntriesRequest request) {
        return dispatchWithFaults(destination, request, () -> delegate.appendEntries(destination, request));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(NodeId destination, InstallSnapshotRequest request) {
        return dispatchWithFaults(destination, request, () -> delegate.installSnapshot(destination, request));
    }

    private <T> CompletableFuture<T> dispatchWithFaults(NodeId destination, Object request, Supplier<CompletableFuture<T>> action) {
        Class<?> rpcClass = request.getClass();
        NodeId source = localNodeId();

        for (FaultRule rule : rules) {
            if (rule.matches(source, destination, rpcClass)) {
                double roll = random.nextDouble();
                if (roll < rule.probability()) {
                    switch (rule.type()) {
                        case PARTITION -> {
                            partitionBlocked.incrementAndGet();
                            log.debug("[CHAOS PARTITION] Dropping RPC {} from {} to {}", rpcClass.getSimpleName(), source, destination);
                            CompletableFuture<T> failed = new CompletableFuture<>();
                            failed.completeExceptionally(new TransportException(ErrorCode.NETWORK_ERROR, "Network partition between " + source + " and " + destination));
                            return failed;
                        }
                        case DROP -> {
                            droppedMessages.incrementAndGet();
                            log.debug("[CHAOS DROP] Discarding RPC {} from {} to {}", rpcClass.getSimpleName(), source, destination);
                            // Simulates dropped message resulting in timeout
                            return new CompletableFuture<>();
                        }
                        case DELAY -> {
                            delayedMessages.incrementAndGet();
                            log.debug("[CHAOS DELAY] Delaying RPC {} from {} to {} by {}ms",
                                    rpcClass.getSimpleName(), source, destination, rule.delay().toMillis());
                            CompletableFuture<T> delayedFuture = new CompletableFuture<>();
                            delayScheduler.schedule(() -> {
                                action.get().whenComplete((res, ex) -> {
                                    if (ex != null) {
                                        delayedFuture.completeExceptionally(ex);
                                    } else {
                                        delayedFuture.complete(res);
                                    }
                                });
                            }, rule.delay().toMillis(), TimeUnit.MILLISECONDS);
                            return delayedFuture;
                        }
                        case DUPLICATE -> {
                            duplicatedMessages.incrementAndGet();
                            log.debug("[CHAOS DUPLICATE] Duplicating RPC {} from {} to {}", rpcClass.getSimpleName(), source, destination);
                            // Fire extra delivery asynchronously
                            for (int i = 0; i < rule.duplicateCount(); i++) {
                                CompletableFuture<T> extra = action.get();
                                extra.whenComplete((r, ex) -> {
                                    // extra duplicate response ignored
                                });
                            }
                            return action.get();
                        }
                        case CORRUPT -> {
                            log.debug("[CHAOS CORRUPT] Simulating corrupted wire message from {} to {}", source, destination);
                            CompletableFuture<T> corrupted = new CompletableFuture<>();
                            corrupted.completeExceptionally(new TransportException(ErrorCode.NETWORK_ERROR, "Corrupted wire payload in RPC " + rpcClass.getSimpleName()));
                            return corrupted;
                        }
                    }
                }
            }
        }

        return action.get();
    }

    @Override
    public void registerHandler(RaftRequestHandler handler) {
        delegate.registerHandler(handler);
    }

    @Override
    public void start() throws IOException {
        delegate.start();
    }

    @Override
    public void stop() {
        delegate.stop();
        if (ownsScheduler) {
            delayScheduler.shutdownNow();
        }
    }

    public RaftTransport delegate() {
        return delegate;
    }
}
