package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.client.ShardedAegisDbClient;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.raft.statemachine.KvCommand;
import se.mouaz.aegisdb.sharding.QueryRouter;
import se.mouaz.aegisdb.sharding.ShardManager;
import se.mouaz.aegisdb.transaction.IsolationLevel;
import se.mouaz.aegisdb.transaction.Transaction;
import se.mouaz.aegisdb.transaction.TransactionManager;
import se.mouaz.aegisdb.transaction.WriteOperation;
import se.mouaz.aegisdb.transaction.distributed.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Formal Milestone M4 Gate Verification Test (Master Project Plan §10, §20; US015):
 * "Milestone M4 after Phase 9: Cross-shard transactions commit or abort atomically via 2PC across multiple Raft groups."
 *
 * Verifies:
 * 1. Cross-shard atomic commit and abort across 3 distinct shards.
 * 2. Strict isolation and key-level prepare locking during in-doubt windows.
 * 3. Crash recovery replaying coordinator journal to resolve in-doubt state.
 * 4. High-concurrency financial invariant: balance conservation A + B + C = 3000 across 3 discrete shards.
 */
class CrossShardTransactionIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(CrossShardTransactionIntegrationTest.class);

    @TempDir
    Path tempDir;

    private ShardId shard0;
    private ShardId shard1;
    private ShardId shard2;

    private MvccStore store0;
    private MvccStore store1;
    private MvccStore store2;

    private TransactionManager tm0;
    private TransactionManager tm1;
    private TransactionManager tm2;

    private LocalShardParticipant participant0;
    private LocalShardParticipant participant1;
    private LocalShardParticipant participant2;

    private DurableCoordinatorLog coordinatorLog;
    private ShardedAegisDbClient client;
    private ShardManager shardManager;

    private String keyA; // mapped to shard-0
    private String keyB; // mapped to shard-1
    private String keyC; // mapped to shard-2

    @BeforeEach
    void setUp() throws IOException {
        shard0 = ShardId.of("shard-0");
        shard1 = ShardId.of("shard-1");
        shard2 = ShardId.of("shard-2");

        store0 = new MvccStore();
        store1 = new MvccStore();
        store2 = new MvccStore();

        tm0 = new TransactionManager(store0);
        tm1 = new TransactionManager(store1);
        tm2 = new TransactionManager(store2);

        participant0 = new LocalShardParticipant(shard0, tm0);
        participant1 = new LocalShardParticipant(shard1, tm1);
        participant2 = new LocalShardParticipant(shard2, tm2);

        Map<ShardId, TransactionParticipant> participants = Map.of(
                shard0, participant0,
                shard1, participant1,
                shard2, participant2
        );

        List<NodeId> nodes = List.of(NodeId.of("node-1"), NodeId.of("node-2"), NodeId.of("node-3"));
        shardManager = ShardManager.createStaticShards(3, nodes, 1);

        QueryRouter.ShardNodeInvoker invoker = (sId, targetNode, cmdBytes) -> {
            KvCommand cmd = KvCommand.fromBytes(cmdBytes);
            MvccStore targetStore;
            if (sId.equals(shard0)) {
                targetStore = store0;
            } else if (sId.equals(shard1)) {
                targetStore = store1;
            } else {
                targetStore = store2;
            }

            return switch (cmd.opType()) {
                case PUT -> {
                    targetStore.put(cmd.key(), cmd.value());
                    yield CompletableFuture.completedFuture(new byte[0]);
                }
                case GET -> {
                    byte[] val = targetStore.get(cmd.key()).orElse(null);
                    yield CompletableFuture.completedFuture(val != null ? val : new byte[0]);
                }
                case DELETE -> {
                    targetStore.delete(cmd.key());
                    yield CompletableFuture.completedFuture(new byte[0]);
                }
                default -> CompletableFuture.failedFuture(new IllegalArgumentException("Unknown cmd"));
            };
        };

        QueryRouter queryRouter = new QueryRouter(shardManager.router(), shardManager.leaderLocator(), invoker);

        Path logPath = tempDir.resolve("coordinator-2pc.log");
        coordinatorLog = new DurableCoordinatorLog(logPath);

        client = new ShardedAegisDbClient(
                queryRouter,
                Collections.emptyMap(),
                participants,
                coordinatorLog
        );

        // Deterministically find 3 distinct keys belonging to the 3 distinct shards
        keyA = findKeyForShard(shard0, "account-A");
        keyB = findKeyForShard(shard1, "account-B");
        keyC = findKeyForShard(shard2, "account-C");

        assertThat(client.resolveShard(keyA)).isEqualTo(shard0);
        assertThat(client.resolveShard(keyB)).isEqualTo(shard1);
        assertThat(client.resolveShard(keyC)).isEqualTo(shard2);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @DisplayName("Milestone M4 Gate: 2PC Atomicity across 3 Shards - all writes commit or abort completely")
    void crossShardAtomicityAllOrNothing() {
        client.putString(keyA, "1000").join();
        client.putString(keyB, "1000").join();
        client.putString(keyC, "1000").join();

        // 1. Transaction aborts midway across all 3 shards
        try (Transaction tx = client.beginTransaction(IsolationLevel.SERIALIZABLE)) {
            tx.putString(keyA, "900");
            tx.putString(keyB, "1050");
            tx.putString(keyC, "1050");
            tx.abort();
        }

        // Must observe original balances
        assertThat(client.getString(keyA).join()).contains("1000");
        assertThat(client.getString(keyB).join()).contains("1000");
        assertThat(client.getString(keyC).join()).contains("1000");

        // Key-level locks must be completely freed
        assertThat(participant0.isKeyLocked(keyA)).isFalse();
        assertThat(participant1.isKeyLocked(keyB)).isFalse();
        assertThat(participant2.isKeyLocked(keyC)).isFalse();

        // 2. Transaction commits successfully across all 3 shards
        try (Transaction tx = client.beginTransaction(IsolationLevel.SERIALIZABLE)) {
            tx.putString(keyA, "900");
            tx.putString(keyB, "1050");
            tx.putString(keyC, "1050");
            tx.commit();
        }

        assertThat(client.getString(keyA).join()).contains("900");
        assertThat(client.getString(keyB).join()).contains("1050");
        assertThat(client.getString(keyC).join()).contains("1050");

        // Locks must be released post commit
        assertThat(participant0.isKeyLocked(keyA)).isFalse();
        assertThat(participant1.isKeyLocked(keyB)).isFalse();
        assertThat(participant2.isKeyLocked(keyC)).isFalse();
    }

    @Test
    @DisplayName("Milestone M4 Gate: Key-Level Prepare Locks guarantee serializability during in-doubt window")
    void prepareLocksEnforceSerializability() {
        client.putString(keyA, "500").join();

        // Participant manually prepares a write on keyA
        TransactionId externalTx = TransactionId.of(8888L);
        participant0.prepare(new PrepareRequest(
                externalTx,
                shard0,
                List.of(WriteOperation.put(keyA, "600".getBytes(StandardCharsets.UTF_8))),
                1L
        ));

        assertThat(participant0.isKeyLocked(keyA)).isTrue();

        // Concurrent client transaction attempting to write keyA must fail preparation
        try (Transaction tx = client.beginTransaction(IsolationLevel.SERIALIZABLE)) {
            tx.putString(keyA, "700");
            org.junit.jupiter.api.Assertions.assertThrows(DistributedTransactionAbortedException.class, tx::commit);
        }

        // Release external transaction
        participant0.abort(externalTx);
        assertThat(participant0.isKeyLocked(keyA)).isFalse();

        // Subsequent client transaction can now acquire and commit
        try (Transaction tx = client.beginTransaction(IsolationLevel.SERIALIZABLE)) {
            tx.putString(keyA, "700");
            tx.commit();
        }

        assertThat(client.getString(keyA).join()).contains("700");
    }

    @Test
    @DisplayName("Milestone M4 Gate: Durable Coordinator Crash Recovery resolves in-doubt transactions")
    void crashRecoveryResolvesInDoubtTransactions() throws IOException {
        Path crashLogPath = tempDir.resolve("crash-recovery.log");
        DurableCoordinatorLog diskLog = new DurableCoordinatorLog(crashLogPath);

        TransactionId inDoubtTx = TransactionId.of(9999L);
        Set<ShardId> involved = Set.of(shard0, shard1, shard2);

        // Prepare participants
        participant0.prepare(new PrepareRequest(inDoubtTx, shard0, List.of(WriteOperation.put(keyA, "111".getBytes(StandardCharsets.UTF_8))), 1L));
        participant1.prepare(new PrepareRequest(inDoubtTx, shard1, List.of(WriteOperation.put(keyB, "222".getBytes(StandardCharsets.UTF_8))), 1L));
        participant2.prepare(new PrepareRequest(inDoubtTx, shard2, List.of(WriteOperation.put(keyC, "333".getBytes(StandardCharsets.UTF_8))), 1L));

        // Crash after COMMIT_DECIDED logged durably
        diskLog.logState(inDoubtTx, TwoPhaseCommitState.COMMIT_DECIDED, involved);
        diskLog.close();

        // Replay and recover
        DurableCoordinatorLog recoveredLog = new DurableCoordinatorLog(crashLogPath);
        Map<ShardId, TransactionParticipant> participants = Map.of(
                shard0, participant0,
                shard1, participant1,
                shard2, participant2
        );

        DistributedTransactionRecovery recovery = new DistributedTransactionRecovery(recoveredLog, participants::get);
        recovery.recover();

        // In-doubt transaction must be completed (COMMITTED)
        assertThat(recoveredLog.getEntry(inDoubtTx).map(CoordinatorLogEntry::state)).contains(TwoPhaseCommitState.COMMITTED);
        assertThat(store0.get(keyA)).isPresent();
        assertThat(new String(store0.get(keyA).get(), StandardCharsets.UTF_8)).isEqualTo("111");
        assertThat(store1.get(keyB)).isPresent();
        assertThat(new String(store1.get(keyB).get(), StandardCharsets.UTF_8)).isEqualTo("222");
        assertThat(store2.get(keyC)).isPresent();
        assertThat(new String(store2.get(keyC).get(), StandardCharsets.UTF_8)).isEqualTo("333");

        // Locks cleared
        assertThat(participant0.isKeyLocked(keyA)).isFalse();
        assertThat(participant1.isKeyLocked(keyB)).isFalse();
        assertThat(participant2.isKeyLocked(keyC)).isFalse();
        recoveredLog.close();
    }

    @Test
    @DisplayName("Milestone M4 Gate: Bank invariant A + B + C = 3000 under high concurrent multi-client cross-shard load")
    void milestoneCrossShardBankTransferConcurrency() throws InterruptedException {
        // Initialize balances on 3 distinct shards: A = 1000 (shard-0), B = 1000 (shard-1), C = 1000 (shard-2)
        client.putString(keyA, "1000").join();
        client.putString(keyB, "1000").join();
        client.putString(keyC, "1000").join();

        int clientThreads = 8;
        int opsPerThread = 60;
        int expectedTotalOps = clientThreads * opsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(clientThreads);
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch doneSignal = new CountDownLatch(clientThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        String[] accounts = {keyA, keyB, keyC};

        for (int i = 0; i < clientThreads; i++) {
            executor.submit(() -> {
                try {
                    startSignal.await();
                    for (int j = 0; j < opsPerThread; j++) {
                        int f = ThreadLocalRandom.current().nextInt(3);
                        int t = (f + 1 + ThreadLocalRandom.current().nextInt(2)) % 3;
                        String from = accounts[f];
                        String to = accounts[t];
                        int amount = ThreadLocalRandom.current().nextInt(1, 25);

                        client.runInTransaction(IsolationLevel.SERIALIZABLE, tx -> {
                            int fromBal = Integer.parseInt(tx.getString(from).orElse("0"));
                            int toBal = Integer.parseInt(tx.getString(to).orElse("0"));

                            tx.putString(from, String.valueOf(fromBal - amount));
                            tx.putString(to, String.valueOf(toBal + amount));
                            return null;
                        }, 50);

                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("Cross-shard concurrent transfer failed", e);
                } finally {
                    doneSignal.countDown();
                }
            });
        }

        startSignal.countDown();
        boolean completed = doneSignal.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(successCount.get()).isEqualTo(expectedTotalOps);

        int finalA = Integer.parseInt(client.getString(keyA).join().orElseThrow());
        int finalB = Integer.parseInt(client.getString(keyB).join().orElseThrow());
        int finalC = Integer.parseInt(client.getString(keyC).join().orElseThrow());

        int totalSum = finalA + finalB + finalC;
        log.info("Milestone M4 Cross-Shard Bank Balances: A={}, B={}, C={} | Total={}", finalA, finalB, finalC, totalSum);

        assertThat(totalSum)
                .as("Milestone M4 Gate Invariant: Financial conservation A + B + C = 3000 strictly preserved across 3 shards")
                .isEqualTo(3000);
    }

    private String findKeyForShard(ShardId targetShard, String prefix) {
        for (int i = 0; i < 5000; i++) {
            String candidate = prefix + ":" + i;
            if (shardManager.router().routeToShardId(candidate).equals(targetShard)) {
                return candidate;
            }
        }
        throw new IllegalStateException("Unable to locate key mapping to " + targetShard);
    }
}
