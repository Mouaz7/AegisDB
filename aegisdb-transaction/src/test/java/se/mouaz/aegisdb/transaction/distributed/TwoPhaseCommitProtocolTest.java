package se.mouaz.aegisdb.transaction.distributed;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.transaction.TransactionManager;
import se.mouaz.aegisdb.transaction.WriteOperation;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TwoPhaseCommitProtocolTest {

    private ShardId shard0;
    private ShardId shard1;
    private MvccStore mvccStore0;
    private MvccStore mvccStore1;
    private TransactionManager tm0;
    private TransactionManager tm1;
    private LocalShardParticipant participant0;
    private LocalShardParticipant participant1;
    private InMemoryCoordinatorLog coordinatorLog;
    private DistributedTransactionCoordinator coordinator;

    @BeforeEach
    void setUp() {
        shard0 = ShardId.of("shard-0");
        shard1 = ShardId.of("shard-1");

        mvccStore0 = new MvccStore();
        mvccStore1 = new MvccStore();

        tm0 = new TransactionManager(mvccStore0);
        tm1 = new TransactionManager(mvccStore1);

        participant0 = new LocalShardParticipant(shard0, tm0);
        participant1 = new LocalShardParticipant(shard1, tm1);

        Map<ShardId, TransactionParticipant> participants = Map.of(
                shard0, participant0,
                shard1, participant1
        );

        coordinatorLog = new InMemoryCoordinatorLog();
        coordinator = new DistributedTransactionCoordinator(coordinatorLog, participants::get, Duration.ofMillis(800), null);
    }

    @AfterEach
    void tearDown() {
        coordinator.close();
    }

    @Test
    void testHappyPathTwoPhaseCommit() {
        TransactionId txId = TransactionId.of(1001);

        Map<ShardId, List<WriteOperation>> writes = new HashMap<>();
        writes.put(shard0, List.of(WriteOperation.put("account:alice", "500".getBytes(StandardCharsets.UTF_8))));
        writes.put(shard1, List.of(WriteOperation.put("account:bob", "700".getBytes(StandardCharsets.UTF_8))));

        coordinator.commit(txId, writes, System.currentTimeMillis()).join();

        assertThat(coordinator.getState(txId)).isEqualTo(TwoPhaseCommitState.COMMITTED);

        // Verify values are committed in both MVCC stores
        Optional<byte[]> aliceVal = mvccStore0.get("account:alice");
        Optional<byte[]> bobVal = mvccStore1.get("account:bob");

        assertThat(aliceVal).isPresent();
        assertThat(new String(aliceVal.get(), StandardCharsets.UTF_8)).isEqualTo("500");

        assertThat(bobVal).isPresent();
        assertThat(new String(bobVal.get(), StandardCharsets.UTF_8)).isEqualTo("700");
    }

    @Test
    void testParticipantRejectsPrepareCausesAbort() {
        TransactionId txId = TransactionId.of(1002);

        // Create a custom participant that always rejects
        TransactionParticipant rejectingParticipant = new TransactionParticipant() {
            @Override
            public CompletableFuture<ParticipantVote> prepare(PrepareRequest request) {
                return CompletableFuture.completedFuture(ParticipantVote.ABORT);
            }

            @Override
            public CompletableFuture<Void> commit(TransactionId txId) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> abort(TransactionId txId) {
                return CompletableFuture.completedFuture(null);
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

        DistributedTransactionCoordinator rejectingCoordinator = new DistributedTransactionCoordinator(
                coordinatorLog,
                s -> s.equals(shard0) ? participant0 : rejectingParticipant
        );

        Map<ShardId, List<WriteOperation>> writes = Map.of(
                shard0, List.of(WriteOperation.put("k0", "val0".getBytes(StandardCharsets.UTF_8))),
                shard1, List.of(WriteOperation.put("k1", "val1".getBytes(StandardCharsets.UTF_8)))
        );

        assertThatThrownBy(() -> rejectingCoordinator.commit(txId, writes, System.currentTimeMillis()).join())
                .hasCauseInstanceOf(DistributedTransactionAbortedException.class);

        assertThat(rejectingCoordinator.getState(txId)).isEqualTo(TwoPhaseCommitState.ABORTED);

        // Verify shard 0 rolled back and released prepare locks
        assertThat(participant0.isKeyLocked("k0")).isFalse();
        assertThat(mvccStore0.get("k0")).isEmpty();

        rejectingCoordinator.close();
    }

    @Test
    void testPrepareTimeoutCausesAbort() {
        TransactionId txId = TransactionId.of(1003);

        // Hanging participant
        TransactionParticipant hangingParticipant = new TransactionParticipant() {
            @Override
            public CompletableFuture<ParticipantVote> prepare(PrepareRequest request) {
                return new CompletableFuture<>(); // never completes
            }

            @Override
            public CompletableFuture<Void> commit(TransactionId txId) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<Void> abort(TransactionId txId) {
                return CompletableFuture.completedFuture(null);
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

        DistributedTransactionCoordinator timingOutCoordinator = new DistributedTransactionCoordinator(
                coordinatorLog,
                s -> s.equals(shard0) ? participant0 : hangingParticipant,
                Duration.ofMillis(100),
                null
        );

        Map<ShardId, List<WriteOperation>> writes = Map.of(
                shard0, List.of(WriteOperation.put("k0", "val0".getBytes(StandardCharsets.UTF_8))),
                shard1, List.of(WriteOperation.put("k1", "val1".getBytes(StandardCharsets.UTF_8)))
        );

        assertThatThrownBy(() -> timingOutCoordinator.commit(txId, writes, System.currentTimeMillis()).join())
                .hasCauseInstanceOf(DistributedTransactionAbortedException.class);

        assertThat(timingOutCoordinator.getState(txId)).isEqualTo(TwoPhaseCommitState.ABORTED);
        assertThat(participant0.isKeyLocked("k0")).isFalse();

        timingOutCoordinator.close();
    }

    @Test
    void testKeyLevelLockingPreventsConcurrentPrepare() {
        TransactionId tx1 = TransactionId.of(2001);
        TransactionId tx2 = TransactionId.of(2002);

        // Tx1 prepares key "shared_account" on shard 0
        PrepareRequest req1 = new PrepareRequest(
                tx1, shard0,
                List.of(WriteOperation.put("shared_account", "100".getBytes(StandardCharsets.UTF_8))),
                System.currentTimeMillis()
        );

        ParticipantVote vote1 = participant0.prepare(req1).join();
        assertThat(vote1).isEqualTo(ParticipantVote.PREPARED);
        assertThat(participant0.isKeyLocked("shared_account")).isTrue();

        // Tx2 attempts to prepare overlapping key "shared_account" on shard 0 while Tx1 is in-doubt
        PrepareRequest req2 = new PrepareRequest(
                tx2, shard0,
                List.of(WriteOperation.put("shared_account", "200".getBytes(StandardCharsets.UTF_8))),
                System.currentTimeMillis()
        );

        ParticipantVote vote2 = participant0.prepare(req2).join();
        assertThat(vote2).isEqualTo(ParticipantVote.ABORT); // Must reject concurrent conflicting prepare!

        // Now Tx1 commits
        participant0.commit(tx1).join();
        assertThat(participant0.isKeyLocked("shared_account")).isFalse();

        // After Tx1 commit and lock release, a new Tx3 can prepare successfully
        TransactionId tx3 = TransactionId.of(2003);
        PrepareRequest req3 = new PrepareRequest(
                tx3, shard0,
                List.of(WriteOperation.put("shared_account", "300".getBytes(StandardCharsets.UTF_8))),
                System.currentTimeMillis()
        );
        ParticipantVote vote3 = participant0.prepare(req3).join();
        assertThat(vote3).isEqualTo(ParticipantVote.PREPARED);
        participant0.commit(tx3).join();
    }

    @Test
    void testIdempotentCommitAndAbort() {
        TransactionId txId = TransactionId.of(3001);
        PrepareRequest req = new PrepareRequest(
                txId, shard0,
                List.of(WriteOperation.put("k_idem", "val_idem".getBytes(StandardCharsets.UTF_8))),
                System.currentTimeMillis()
        );

        participant0.prepare(req).join();

        // Commit once
        participant0.commit(txId).join();
        // Commit twice (idempotent)
        participant0.commit(txId).join();

        assertThat(mvccStore0.get("k_idem")).isPresent();

        // Abort on committed tx is idempotent no-op
        participant0.abort(txId).join();
        assertThat(mvccStore0.get("k_idem")).isPresent();
    }
}
