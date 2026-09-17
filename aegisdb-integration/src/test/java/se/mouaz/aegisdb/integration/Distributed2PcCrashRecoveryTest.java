package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.storage.wal.FsyncPolicy;
import se.mouaz.aegisdb.storage.wal.WalConfig;
import se.mouaz.aegisdb.transaction.TransactionManager;
import se.mouaz.aegisdb.transaction.WriteOperation;
import se.mouaz.aegisdb.transaction.distributed.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Distributed 2PC Crash Recovery Invariants & State Transitions")
class Distributed2PcCrashRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("2PC Coordinator crash after COMMIT_DECIDED forces global commit on recovery")
    void crashAfterCommitDecidedReplaysCommitToParticipants() throws Exception {
        Path coordLogDir = tempDir.resolve("coord-commit-log");

        ShardId shard1 = ShardId.of("shard-alpha");
        ShardId shard2 = ShardId.of("shard-beta");
        Set<ShardId> participants = Set.of(shard1, shard2);

        MvccStore store1 = new MvccStore();
        MvccStore store2 = new MvccStore();
        TransactionManager tm1 = new TransactionManager(store1);
        TransactionManager tm2 = new TransactionManager(store2);

        LocalShardParticipant p1 = new LocalShardParticipant(shard1, tm1);
        LocalShardParticipant p2 = new LocalShardParticipant(shard2, tm2);
        Map<ShardId, TransactionParticipant> participantMap = Map.of(shard1, p1, shard2, p2);

        TransactionId txId = TransactionId.of(901L);

        // 1. Prepare phase on participants
        List<WriteOperation> writes1 = List.of(WriteOperation.put("alpha:key1", "val1".getBytes(StandardCharsets.UTF_8)));
        List<WriteOperation> writes2 = List.of(WriteOperation.put("beta:key2", "val2".getBytes(StandardCharsets.UTF_8)));

        assertThat(p1.prepare(new PrepareRequest(txId, shard1, writes1, 0L)).get(3, TimeUnit.SECONDS).isPrepared()).isTrue();
        assertThat(p2.prepare(new PrepareRequest(txId, shard2, writes2, 0L)).get(3, TimeUnit.SECONDS).isPrepared()).isTrue();

        Path logFile = coordLogDir.resolve("coord.log");

        // 2. Write COMMIT_DECIDED to durable coordinator log and simulate crash before commit replay completes
        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logFile)) {
            log.logState(txId, TwoPhaseCommitState.COMMIT_DECIDED, participants);
        }

        // 3. Restart and recover from persistent coordinator log
        try (DurableCoordinatorLog recoveredLog = new DurableCoordinatorLog(logFile)) {
            DistributedTransactionRecovery recovery = new DistributedTransactionRecovery(recoveredLog, participantMap::get);
            DistributedTransactionRecovery.RecoverySummary summary = recovery.recover();

            assertThat(summary.recoveredTransactions()).isEqualTo(1);
            assertThat(summary.completedCommits()).isEqualTo(1);
            assertThat(summary.completedAborts()).isZero();

            // Coordinator log must now record terminal COMMITTED
            Map<TransactionId, CoordinatorLogEntry> entries = recoveredLog.recover();
            assertThat(entries.get(txId).state()).isEqualTo(TwoPhaseCommitState.COMMITTED);

            // Both participants must have committed their data into MVCC store
            assertThat(store1.get("alpha:key1")).isPresent();
            assertThat(new String(store1.get("alpha:key1").get(), StandardCharsets.UTF_8)).isEqualTo("val1");

            assertThat(store2.get("beta:key2")).isPresent();
            assertThat(new String(store2.get("beta:key2").get(), StandardCharsets.UTF_8)).isEqualTo("val2");
        }
    }

    @Test
    @DisplayName("2PC Coordinator crash during PREPARING (pre-commit) forces global abort on recovery")
    void crashBeforeCommitDecisionRollsBackParticipants() throws Exception {
        Path coordLogDir = tempDir.resolve("coord-abort-log");
        Path logFile = coordLogDir.resolve("coord.log");

        ShardId shard1 = ShardId.of("shard-alpha");
        ShardId shard2 = ShardId.of("shard-beta");
        Set<ShardId> participants = Set.of(shard1, shard2);

        MvccStore store1 = new MvccStore();
        MvccStore store2 = new MvccStore();
        TransactionManager tm1 = new TransactionManager(store1);
        TransactionManager tm2 = new TransactionManager(store2);

        LocalShardParticipant p1 = new LocalShardParticipant(shard1, tm1);
        LocalShardParticipant p2 = new LocalShardParticipant(shard2, tm2);
        Map<ShardId, TransactionParticipant> participantMap = Map.of(shard1, p1, shard2, p2);

        TransactionId txId = TransactionId.of(902L);

        // Prepare participants
        List<WriteOperation> writes1 = List.of(WriteOperation.put("alpha:k", "v".getBytes(StandardCharsets.UTF_8)));
        List<WriteOperation> writes2 = List.of(WriteOperation.put("beta:k", "v".getBytes(StandardCharsets.UTF_8)));

        p1.prepare(new PrepareRequest(txId, shard1, writes1, 0L)).get(3, TimeUnit.SECONDS);
        p2.prepare(new PrepareRequest(txId, shard2, writes2, 0L)).get(3, TimeUnit.SECONDS);

        // Crash occurs while PREPARING (before decision is reached)
        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logFile)) {
            log.logState(txId, TwoPhaseCommitState.PREPARING, participants);
        }

        // Recovery resolves in-doubt state to ABORT
        try (DurableCoordinatorLog recoveredLog = new DurableCoordinatorLog(logFile)) {
            DistributedTransactionRecovery recovery = new DistributedTransactionRecovery(recoveredLog, participantMap::get);
            DistributedTransactionRecovery.RecoverySummary summary = recovery.recover();

            assertThat(summary.recoveredTransactions()).isEqualTo(1);
            assertThat(summary.completedAborts()).isEqualTo(1);
            assertThat(summary.completedCommits()).isZero();

            // State transitioned to ABORTED
            Map<TransactionId, CoordinatorLogEntry> entries = recoveredLog.recover();
            assertThat(entries.get(txId).state()).isEqualTo(TwoPhaseCommitState.ABORTED);

            // Neither store contains the uncommitted write
            assertThat(store1.get("alpha:k")).isEmpty();
            assertThat(store2.get("beta:k")).isEmpty();
        }
    }
}
