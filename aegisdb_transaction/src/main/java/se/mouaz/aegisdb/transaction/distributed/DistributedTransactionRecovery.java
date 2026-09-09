package se.mouaz.aegisdb.transaction.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Distributed crash recovery engine for Two-Phase Commit state transitions (Master Project Plan §10; US015).
 * Replays coordinator journal on startup and resolves in-doubt transactions across the 8 failure modes.
 */
public class DistributedTransactionRecovery {
    private static final Logger log = LoggerFactory.getLogger(DistributedTransactionRecovery.class);

    private final TransactionCoordinatorLog coordinatorLog;
    private final Function<ShardId, TransactionParticipant> participantLookup;

    public DistributedTransactionRecovery(
            TransactionCoordinatorLog coordinatorLog,
            Function<ShardId, TransactionParticipant> participantLookup
    ) {
        this.coordinatorLog = Objects.requireNonNull(coordinatorLog, "coordinatorLog cannot be null");
        this.participantLookup = Objects.requireNonNull(participantLookup, "participantLookup cannot be null");
    }

    public record RecoverySummary(
            int recoveredTransactions,
            int completedCommits,
            int completedAborts,
            int skippedTerminal
    ) {}

    /**
     * Executes recovery on startup by reading the coordinator log and resolving any non-terminal transactions.
     */
    public RecoverySummary recover() {
        log.info("Starting distributed transaction recovery from coordinator log...");
        Map<TransactionId, CoordinatorLogEntry> logEntries = coordinatorLog.recover();

        int recoveredCount = 0;
        int commitsResolved = 0;
        int abortsResolved = 0;
        int terminalSkipped = 0;

        for (Map.Entry<TransactionId, CoordinatorLogEntry> entry : logEntries.entrySet()) {
            TransactionId txId = entry.getKey();
            CoordinatorLogEntry logEntry = entry.getValue();
            TwoPhaseCommitState state = logEntry.state();

            log.info("Recovering txId={} with state={} across shards={}", txId, state, logEntry.participants());

            switch (state) {
                case COMMITTED, ABORTED -> {
                    // Terminal states require no recovery
                    terminalSkipped++;
                }

                case COMMIT_DECIDED -> {
                    // Case 2: Crashed after deciding COMMIT. Invariant: MUST commit on all participants.
                    recoveredCount++;
                    resolveCommit(txId, logEntry);
                    commitsResolved++;
                }

                case ABORT_DECIDED -> {
                    // Case 3: Crashed after deciding ABORT. Re-drive abort to all participants.
                    recoveredCount++;
                    resolveAbort(txId, logEntry);
                    abortsResolved++;
                }

                case INIT, PREPARING -> {
                    // Case 1: Crashed before commit decision was durably recorded. Invariant: MUST abort.
                    recoveredCount++;
                    coordinatorLog.logState(txId, TwoPhaseCommitState.ABORT_DECIDED, logEntry.participants());
                    resolveAbort(txId, logEntry);
                    abortsResolved++;
                }
            }
        }

        RecoverySummary summary = new RecoverySummary(recoveredCount, commitsResolved, abortsResolved, terminalSkipped);
        log.info("Distributed transaction recovery complete: {}", summary);
        return summary;
    }

    private void resolveCommit(TransactionId txId, CoordinatorLogEntry entry) {
        log.info("Recovery: re-driving COMMIT for txId={}", txId);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (ShardId shardId : entry.participants()) {
            TransactionParticipant participant = participantLookup.apply(shardId);
            if (participant != null) {
                futures.add(participant.commit(txId).exceptionally(ex -> {
                    log.error("Recovery commit failed for shard {} txId={}: {}", shardId, txId, ex.getMessage());
                    return null;
                }));
            }
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        coordinatorLog.logState(txId, TwoPhaseCommitState.COMMITTED, entry.participants());
    }

    private void resolveAbort(TransactionId txId, CoordinatorLogEntry entry) {
        log.info("Recovery: re-driving ABORT for txId={}", txId);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (ShardId shardId : entry.participants()) {
            TransactionParticipant participant = participantLookup.apply(shardId);
            if (participant != null) {
                futures.add(participant.abort(txId).exceptionally(ex -> {
                    log.warn("Recovery abort failed for shard {} txId={}: {}", shardId, txId, ex.getMessage());
                    return null;
                }));
            }
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        coordinatorLog.logState(txId, TwoPhaseCommitState.ABORTED, entry.participants());
    }
}
