package se.mouaz.aegisdb.transaction.distributed;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.transaction.TransactionManager;
import se.mouaz.aegisdb.transaction.WriteOperation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the 8 failure modes of the Two-Phase Commit Distributed Crash Recovery Matrix (Master Project Plan §10; US015).
 */
class DistributedRecoveryFailureMatrixTest {

    @TempDir
    Path tempDir;

    private ShardId shard0;
    private ShardId shard1;
    private MvccStore store0;
    private MvccStore store1;
    private TransactionManager tm0;
    private TransactionManager tm1;
    private LocalShardParticipant participant0;
    private LocalShardParticipant participant1;
    private Path logPath;

    @BeforeEach
    void setUp() {
        shard0 = ShardId.of("shard-0");
        shard1 = ShardId.of("shard-1");

        store0 = new MvccStore();
        store1 = new MvccStore();

        tm0 = new TransactionManager(store0);
        tm1 = new TransactionManager(store1);

        participant0 = new LocalShardParticipant(shard0, tm0);
        participant1 = new LocalShardParticipant(shard1, tm1);

        logPath = tempDir.resolve("coordinator-recovery.wal");
    }

    private Map<ShardId, TransactionParticipant> participantMap() {
        return Map.of(shard0, participant0, shard1, participant1);
    }

    // =========================================================================
    // Scenario 1: Coordinator Crashes in PREPARING (before commit decision)
    // Resolution: Must abort across all participants and record ABORTED
    // =========================================================================
    @Test
    void testScenario1_CoordinatorCrashInPreparing() throws IOException {
        TransactionId txId = TransactionId.of(501);

        // Participants prepared locally and hold locks
        participant0.prepare(new PrepareRequest(txId, shard0, List.of(WriteOperation.put("k0", "v0".getBytes(StandardCharsets.UTF_8))), 100L)).join();
        participant1.prepare(new PrepareRequest(txId, shard1, List.of(WriteOperation.put("k1", "v1".getBytes(StandardCharsets.UTF_8))), 100L)).join();

        assertThat(participant0.isKeyLocked("k0")).isTrue();
        assertThat(participant1.isKeyLocked("k1")).isTrue();

        // Coordinator logged PREPARING and crashed
        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logPath)) {
            log.logState(txId, TwoPhaseCommitState.PREPARING, Set.of(shard0, shard1));
        }

        // Recovery starts
        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logPath)) {
            DistributedTransactionRecovery recovery = new DistributedTransactionRecovery(log, participantMap()::get);
            DistributedTransactionRecovery.RecoverySummary summary = recovery.recover();

            assertThat(summary.recoveredTransactions()).isEqualTo(1);
            assertThat(summary.completedAborts()).isEqualTo(1);
            assertThat(log.getEntry(txId).get().state()).isEqualTo(TwoPhaseCommitState.ABORTED);
        }

        // Locks released and data not committed
        assertThat(participant0.isKeyLocked("k0")).isFalse();
        assertThat(participant1.isKeyLocked("k1")).isFalse();
        assertThat(store0.get("k0")).isEmpty();
        assertThat(store1.get("k1")).isEmpty();
    }

    // =========================================================================
    // Scenario 2: Coordinator Crashes in COMMIT_DECIDED (after logging commit decision)
    // Resolution: Must re-drive commit across all participants and record COMMITTED
    // =========================================================================
    @Test
    void testScenario2_CoordinatorCrashInCommitDecided() throws IOException {
        TransactionId txId = TransactionId.of(502);

        // Participants prepared locally
        participant0.prepare(new PrepareRequest(txId, shard0, List.of(WriteOperation.put("k0", "val0".getBytes(StandardCharsets.UTF_8))), 100L)).join();
        participant1.prepare(new PrepareRequest(txId, shard1, List.of(WriteOperation.put("k1", "val1".getBytes(StandardCharsets.UTF_8))), 100L)).join();

        // Coordinator durably logged COMMIT_DECIDED then crashed
        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logPath)) {
            log.logState(txId, TwoPhaseCommitState.PREPARING, Set.of(shard0, shard1));
            log.logState(txId, TwoPhaseCommitState.COMMIT_DECIDED, Set.of(shard0, shard1));
        }

        // Recovery starts
        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logPath)) {
            DistributedTransactionRecovery recovery = new DistributedTransactionRecovery(log, participantMap()::get);
            DistributedTransactionRecovery.RecoverySummary summary = recovery.recover();

            assertThat(summary.recoveredTransactions()).isEqualTo(1);
            assertThat(summary.completedCommits()).isEqualTo(1);
            assertThat(log.getEntry(txId).get().state()).isEqualTo(TwoPhaseCommitState.COMMITTED);
        }

        // Invariant: Both shards committed and locks released
        assertThat(participant0.isKeyLocked("k0")).isFalse();
        assertThat(participant1.isKeyLocked("k1")).isFalse();
        assertThat(store0.get("k0")).isPresent();
        assertThat(store1.get("k1")).isPresent();
    }

    // =========================================================================
    // Scenario 3: Coordinator Crashes in ABORT_DECIDED
    // Resolution: Must re-drive abort across all participants and record ABORTED
    // =========================================================================
    @Test
    void testScenario3_CoordinatorCrashInAbortDecided() throws IOException {
        TransactionId txId = TransactionId.of(503);

        participant0.prepare(new PrepareRequest(txId, shard0, List.of(WriteOperation.put("k0", "v0".getBytes(StandardCharsets.UTF_8))), 100L)).join();

        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logPath)) {
            log.logState(txId, TwoPhaseCommitState.PREPARING, Set.of(shard0, shard1));
            log.logState(txId, TwoPhaseCommitState.ABORT_DECIDED, Set.of(shard0, shard1));
        }

        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logPath)) {
            DistributedTransactionRecovery recovery = new DistributedTransactionRecovery(log, participantMap()::get);
            DistributedTransactionRecovery.RecoverySummary summary = recovery.recover();

            assertThat(summary.recoveredTransactions()).isEqualTo(1);
            assertThat(summary.completedAborts()).isEqualTo(1);
            assertThat(log.getEntry(txId).get().state()).isEqualTo(TwoPhaseCommitState.ABORTED);
        }

        assertThat(participant0.isKeyLocked("k0")).isFalse();
        assertThat(store0.get("k0")).isEmpty();
    }

    // =========================================================================
    // Scenario 4: Coordinator Replays Terminal States (COMMITTED or ABORTED)
    // Resolution: Skipped, no-op
    // =========================================================================
    @Test
    void testScenario4_TerminalStatesSkipped() throws IOException {
        TransactionId tx1 = TransactionId.of(5041);
        TransactionId tx2 = TransactionId.of(5042);

        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logPath)) {
            log.logState(tx1, TwoPhaseCommitState.COMMITTED, Set.of(shard0));
            log.logState(tx2, TwoPhaseCommitState.ABORTED, Set.of(shard1));
        }

        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logPath)) {
            DistributedTransactionRecovery recovery = new DistributedTransactionRecovery(log, participantMap()::get);
            DistributedTransactionRecovery.RecoverySummary summary = recovery.recover();

            assertThat(summary.recoveredTransactions()).isEqualTo(0);
            assertThat(summary.skippedTerminal()).isEqualTo(2);
        }
    }

    // =========================================================================
    // Scenario 5: Participant Crashes Before Prepare
    // Resolution: Participant returns ABORT or error; coordinator aborts
    // =========================================================================
    @Test
    void testScenario5_ParticipantCrashBeforePrepare() {
        TransactionId txId = TransactionId.of(505);

        // Participant 1 is unresponsive/crashed
        Map<ShardId, TransactionParticipant> crashedMap = Map.of(shard0, participant0);
        InMemoryCoordinatorLog coordLog = new InMemoryCoordinatorLog();
        DistributedTransactionCoordinator coord = new DistributedTransactionCoordinator(coordLog, crashedMap::get);

        Map<ShardId, List<WriteOperation>> writes = Map.of(
                shard0, List.of(WriteOperation.put("k0", "v0".getBytes(StandardCharsets.UTF_8))),
                shard1, List.of(WriteOperation.put("k1", "v1".getBytes(StandardCharsets.UTF_8)))
        );

        try {
            coord.commit(txId, writes, 100L).join();
        } catch (Exception ignored) {
        }

        assertThat(coord.getState(txId)).isEqualTo(TwoPhaseCommitState.ABORTED);
        coord.close();
    }

    // =========================================================================
    // Scenario 6: Participant Crashes After Prepare (In-Doubt state)
    // Resolution: Participant stays locked until Coordinator resolves it
    // =========================================================================
    @Test
    void testScenario6_ParticipantInDoubtResolvedByCoordinator() {
        TransactionId txId = TransactionId.of(506);

        // Participant 0 successfully prepared
        PrepareRequest req = new PrepareRequest(txId, shard0, List.of(WriteOperation.put("k_doubt", "v_doubt".getBytes(StandardCharsets.UTF_8))), 100L);
        participant0.prepare(req).join();

        assertThat(participant0.getPreparedState(txId)).isPresent();
        assertThat(participant0.getPreparedState(txId).get()).isEqualTo(ParticipantVote.PREPARED);
        assertThat(participant0.isKeyLocked("k_doubt")).isTrue();

        // Coordinator sends COMMIT
        participant0.commit(txId).join();

        assertThat(participant0.isKeyLocked("k_doubt")).isFalse();
        assertThat(store0.get("k_doubt")).isPresent();
    }

    // =========================================================================
    // Scenario 7: Participant Crashes After Commit (Re-delivery is idempotent)
    // =========================================================================
    @Test
    void testScenario7_ParticipantRedeliveryIdempotent() {
        TransactionId txId = TransactionId.of(507);

        PrepareRequest req = new PrepareRequest(txId, shard0, List.of(WriteOperation.put("k_idem", "v_idem".getBytes(StandardCharsets.UTF_8))), 100L);
        participant0.prepare(req).join();
        participant0.commit(txId).join();

        // Simulate crash & recovery re-delivery of commit
        participant0.commit(txId).join();

        assertThat(store0.get("k_idem")).isPresent();
    }

    // =========================================================================
    // Scenario 8: Network Partition / Timeout during Prepare
    // Resolution: Coordinator watchdog aborts all participants
    // =========================================================================
    @Test
    void testScenario8_NetworkPartitionWatchdogAborts() {
        TransactionId txId = TransactionId.of(508);
        InMemoryCoordinatorLog coordLog = new InMemoryCoordinatorLog();

        // Shard 1 is partitioned / timing out
        TransactionParticipant partitionedShard = new TransactionParticipant() {
            @Override
            public java.util.concurrent.CompletableFuture<ParticipantVote> prepare(PrepareRequest request) {
                return new java.util.concurrent.CompletableFuture<>(); // never finishes
            }

            @Override
            public java.util.concurrent.CompletableFuture<Void> commit(TransactionId txId) {
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }

            @Override
            public java.util.concurrent.CompletableFuture<Void> abort(TransactionId txId) {
                return java.util.concurrent.CompletableFuture.completedFuture(null);
            }

            @Override
            public ShardId shardId() {
                return shard1;
            }

            @Override
            public Optional<ParticipantVote> getPreparedState(TransactionId txId) {
                return Optional.empty();
            }
        };

        DistributedTransactionCoordinator watchdogCoord = new DistributedTransactionCoordinator(
                coordLog,
                s -> s.equals(shard0) ? participant0 : partitionedShard,
                java.time.Duration.ofMillis(80),
                null
        );

        Map<ShardId, List<WriteOperation>> writes = Map.of(
                shard0, List.of(WriteOperation.put("k0", "v0".getBytes(StandardCharsets.UTF_8))),
                shard1, List.of(WriteOperation.put("k1", "v1".getBytes(StandardCharsets.UTF_8)))
        );

        try {
            watchdogCoord.commit(txId, writes, 100L).join();
        } catch (Exception ignored) {
        }

        assertThat(watchdogCoord.getState(txId)).isEqualTo(TwoPhaseCommitState.ABORTED);
        assertThat(participant0.isKeyLocked("k0")).isFalse();

        watchdogCoord.close();
    }
}
