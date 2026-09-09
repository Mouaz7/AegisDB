package se.mouaz.aegisdb.transaction.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.transaction.WriteOperation;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Two-Phase Commit (2PC) Distributed Transaction Coordinator (Master Project Plan §10; US015, Milestone M4 Gate).
 * Coordinates atomic state transitions across participating shards with durable write-ahead logging.
 */
public class DistributedTransactionCoordinator implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(DistributedTransactionCoordinator.class);

    private final TransactionCoordinatorLog coordinatorLog;
    private final Function<ShardId, TransactionParticipant> participantLookup;
    private final Duration prepareTimeout;
    private final ScheduledExecutorService scheduler;
    private final boolean ownsScheduler;

    // In-flight transactions tracked by coordinator
    private final Map<TransactionId, TwoPhaseCommitState> activeTransactions = new ConcurrentHashMap<>();

    public DistributedTransactionCoordinator(
            TransactionCoordinatorLog coordinatorLog,
            Function<ShardId, TransactionParticipant> participantLookup
    ) {
        this(coordinatorLog, participantLookup, Duration.ofSeconds(5), null);
    }

    public DistributedTransactionCoordinator(
            TransactionCoordinatorLog coordinatorLog,
            Function<ShardId, TransactionParticipant> participantLookup,
            Duration prepareTimeout,
            ScheduledExecutorService scheduler
    ) {
        this.coordinatorLog = Objects.requireNonNull(coordinatorLog, "coordinatorLog cannot be null");
        this.participantLookup = Objects.requireNonNull(participantLookup, "participantLookup cannot be null");
        this.prepareTimeout = Objects.requireNonNull(prepareTimeout, "prepareTimeout cannot be null");
        if (scheduler != null) {
            this.scheduler = scheduler;
            this.ownsScheduler = false;
        } else {
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "2pc-coordinator-scheduler");
                t.setDaemon(true);
                return t;
            });
            this.ownsScheduler = true;
        }
    }

    public TransactionCoordinatorLog getCoordinatorLog() {
        return coordinatorLog;
    }

    /**
     * Executes the Two-Phase Commit protocol for a distributed transaction across target shards.
     *
     * @param txId Transaction identifier
     * @param writesByShard Map of ShardId -> list of write operations staged for that shard
     * @param readTimestamp Snapshot read timestamp
     * @return CompletableFuture completing when the transaction is durably committed or exceptionally on abort
     */
    public CompletableFuture<Void> commit(
            TransactionId txId,
            Map<ShardId, List<WriteOperation>> writesByShard,
            long readTimestamp
    ) {
        return commit(txId, writesByShard, Collections.emptyMap(), readTimestamp);
    }

    /**
     * Executes the Two-Phase Commit protocol with expected reads validation for OCC serializability.
     */
    public CompletableFuture<Void> commit(
            TransactionId txId,
            Map<ShardId, List<WriteOperation>> writesByShard,
            Map<ShardId, Map<String, Optional<byte[]>>> readsByShard,
            long readTimestamp
    ) {
        Objects.requireNonNull(txId, "txId cannot be null");
        Objects.requireNonNull(writesByShard, "writesByShard cannot be null");

        Set<ShardId> participantShards = new LinkedHashSet<>(writesByShard.keySet());
        if (readsByShard != null) {
            participantShards.addAll(readsByShard.keySet());
        }

        if (participantShards.isEmpty()) {
            // Read-only or empty transaction: trivially committed
            return CompletableFuture.completedFuture(null);
        }

        activeTransactions.put(txId, TwoPhaseCommitState.PREPARING);
        coordinatorLog.logState(txId, TwoPhaseCommitState.PREPARING, participantShards);

        log.info("Starting 2PC PREPARE phase for txId={} across shards: {}", txId, participantShards);

        // Phase 1: PREPARE broadcast to all participants in parallel
        List<CompletableFuture<ParticipantVoteResult>> prepareFutures = new ArrayList<>();
        for (ShardId shardId : participantShards) {
            List<WriteOperation> writes = writesByShard.getOrDefault(shardId, Collections.emptyList());
            Map<String, Optional<byte[]>> expectedReads = (readsByShard != null)
                    ? readsByShard.getOrDefault(shardId, Collections.emptyMap())
                    : Collections.emptyMap();

            TransactionParticipant participant = participantLookup.apply(shardId);
            if (participant == null) {
                log.error("Participant for shard {} not found for txId={}", shardId, txId);
                return abort(txId, participantShards, new IllegalStateException("Shard participant not found: " + shardId));
            }

            PrepareRequest req = new PrepareRequest(txId, shardId, writes, expectedReads, readTimestamp);
            CompletableFuture<ParticipantVoteResult> voteFuture = participant.prepare(req)
                    .thenApply(vote -> new ParticipantVoteResult(shardId, vote, null))
                    .exceptionally(ex -> new ParticipantVoteResult(shardId, ParticipantVote.ABORT, ex));

            prepareFutures.add(voteFuture);
        }

        // Collect all votes
        CompletableFuture<List<ParticipantVoteResult>> allVotesFuture = CompletableFuture.allOf(
                prepareFutures.toArray(new CompletableFuture[0])
        ).thenApply(v -> prepareFutures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList())
        );

        // Schedule timeout watchdog
        CompletableFuture<List<ParticipantVoteResult>> timedVotesFuture = withTimeout(allVotesFuture, prepareTimeout);

        return timedVotesFuture.handle((votes, ex) -> {
            if (ex != null) {
                log.warn("2PC Phase 1 PREPARE failed or timed out for txId={}: {}", txId, ex.getMessage());
                return abort(txId, participantShards, ex);
            }

            // Check if all participants voted PREPARED
            boolean allPrepared = true;
            for (ParticipantVoteResult res : votes) {
                if (res.vote() != ParticipantVote.PREPARED) {
                    allPrepared = false;
                    log.warn("Participant for shard {} voted ABORT for txId={} (cause: {})",
                            res.shardId(), txId, res.cause() != null ? res.cause().getMessage() : "explicit abort vote");
                    break;
                }
            }

            if (allPrepared) {
                return proceedToCommit(txId, participantShards);
            } else {
                return abort(txId, participantShards, new DistributedTransactionAbortedException(
                        "One or more shards rejected prepare for txId=" + txId));
            }
        }).thenCompose(Function.identity());
    }

    /**
     * Phase 2: COMMIT decision durably logged, followed by broadcast commit to participants.
     */
    private CompletableFuture<Void> proceedToCommit(TransactionId txId, Set<ShardId> participantShards) {
        log.info("2PC Phase 2: All participants PREPARED. Durably logging COMMIT_DECIDED for txId={}", txId);

        // 1. Durably log commit decision BEFORE broadcasting to participants
        coordinatorLog.logState(txId, TwoPhaseCommitState.COMMIT_DECIDED, participantShards);
        activeTransactions.put(txId, TwoPhaseCommitState.COMMIT_DECIDED);

        // 2. Broadcast commit to all participants
        List<CompletableFuture<Void>> commitFutures = new ArrayList<>();
        for (ShardId shardId : participantShards) {
            TransactionParticipant participant = participantLookup.apply(shardId);
            if (participant != null) {
                commitFutures.add(participant.commit(txId).exceptionally(err -> {
                    log.error("Commit broadcast error to shard {} for txId={}: {}", shardId, txId, err.getMessage());
                    // 2PC invariant: once commit is decided, it MUST succeed; participants retry or recover
                    return null;
                }));
            }
        }

        return CompletableFuture.allOf(commitFutures.toArray(new CompletableFuture[0]))
                .thenRun(() -> {
                    coordinatorLog.logState(txId, TwoPhaseCommitState.COMMITTED, participantShards);
                    activeTransactions.put(txId, TwoPhaseCommitState.COMMITTED);
                    log.info("2PC COMMITTED successfully for txId={}", txId);
                });
    }

    /**
     * Phase 2: ABORT decision durably logged, followed by broadcast abort to participants.
     */
    public CompletableFuture<Void> abort(TransactionId txId, Set<ShardId> participantShards, Throwable cause) {
        log.info("2PC Phase 2: Aborting txId={} across shards: {} (reason: {})",
                txId, participantShards, cause != null ? cause.getMessage() : "manual abort");

        // 1. Durably log abort decision
        coordinatorLog.logState(txId, TwoPhaseCommitState.ABORT_DECIDED, participantShards);
        activeTransactions.put(txId, TwoPhaseCommitState.ABORT_DECIDED);

        // 2. Broadcast abort to all participants
        List<CompletableFuture<Void>> abortFutures = new ArrayList<>();
        for (ShardId shardId : participantShards) {
            TransactionParticipant participant = participantLookup.apply(shardId);
            if (participant != null) {
                abortFutures.add(participant.abort(txId).exceptionally(err -> {
                    log.warn("Abort notification error to shard {} for txId={}: {}", shardId, txId, err.getMessage());
                    return null;
                }));
            }
        }

        return CompletableFuture.allOf(abortFutures.toArray(new CompletableFuture[0]))
                .handle((v, err) -> {
                    coordinatorLog.logState(txId, TwoPhaseCommitState.ABORTED, participantShards);
                    activeTransactions.put(txId, TwoPhaseCommitState.ABORTED);
                    log.info("2PC ABORTED successfully for txId={}", txId);

                    Throwable finalEx = (cause instanceof DistributedTransactionAbortedException)
                            ? cause
                            : new DistributedTransactionAbortedException("Transaction aborted: " + txId, cause);
                    CompletableFuture<Void> failed = new CompletableFuture<>();
                    failed.completeExceptionally(finalEx);
                    return failed;
                }).thenCompose(Function.identity());
    }

    private <T> CompletableFuture<T> withTimeout(CompletableFuture<T> future, Duration timeout) {
        CompletableFuture<T> result = new CompletableFuture<>();
        var scheduledTask = scheduler.schedule(() -> {
            result.completeExceptionally(new TimeoutException("2PC PREPARE timed out after " + timeout.toMillis() + " ms"));
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);

        future.whenComplete((v, ex) -> {
            scheduledTask.cancel(false);
            if (ex != null) {
                result.completeExceptionally(ex);
            } else {
                result.complete(v);
            }
        });

        return result;
    }

    public TwoPhaseCommitState getState(TransactionId txId) {
        TwoPhaseCommitState state = activeTransactions.get(txId);
        if (state != null) {
            return state;
        }
        return coordinatorLog.getEntry(txId).map(CoordinatorLogEntry::state).orElse(TwoPhaseCommitState.INIT);
    }

    @Override
    public void close() {
        if (ownsScheduler) {
            scheduler.shutdownNow();
        }
        try {
            coordinatorLog.close();
        } catch (Exception e) {
            log.warn("Error closing coordinator log: {}", e.getMessage());
        }
    }

    private record ParticipantVoteResult(
            ShardId shardId,
            ParticipantVote vote,
            Throwable cause
    ) {}
}
