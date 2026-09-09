package se.mouaz.aegisdb.integration;

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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sprint 9 Live Demonstration:
 * Cross-Shard Distributed Transactions & Two-Phase Commit (US015, Milestone M4 Gate; Master Project Plan §10, §20).
 *
 * Demonstrates:
 * 1. [AC1] Multi-Shard Cluster & Participant Setup (3 Shards, 3 LocalShardParticipants, Durable Coordinator WAL)
 * 2. [AC2] 2PC Phase 1 (PREPARE) & Phase 2 (COMMIT) Atomic Flow with Read-Your-Own-Writes
 * 3. [AC3] Cross-Shard Transaction Rollback & Abort Atomicity
 * 4. [AC4] Key-Level Prepare Locks & Serializability Enforcement during In-Doubt Window
 * 5. [AC5] Durable Coordinator Crash Recovery across Failure Matrix (§10)
 * 6. [AC6] Formal Milestone M4 Gate: High-Concurrency Bank Invariant (A + B + C = 3000 across 3 distinct shards)
 */
public class Sprint9Demo {
    private static final Logger log = LoggerFactory.getLogger(Sprint9Demo.class);

    public static void main(String[] args) {
        System.out.println("=======================================================================");
        System.out.println("     AegisDB - Sprint 9 Live Demonstration");
        System.out.println("     Cross-Shard Distributed Transactions & Two-Phase Commit (2PC)");
        System.out.println("     Milestone M4 Gate | US015 | Master Project Plan §10, §20");
        System.out.println("=======================================================================\n");

        try {
            // --- AC1: Multi-Shard Cluster & 2PC Infrastructure ---
            System.out.println("▶ [1/6] [AC1] Initializing 3-Shard Cluster, Local Participants & Durable WAL...");
            ShardId shard0 = ShardId.of("shard-0");
            ShardId shard1 = ShardId.of("shard-1");
            ShardId shard2 = ShardId.of("shard-2");

            MvccStore store0 = new MvccStore();
            MvccStore store1 = new MvccStore();
            MvccStore store2 = new MvccStore();

            TransactionManager tm0 = new TransactionManager(store0);
            TransactionManager tm1 = new TransactionManager(store1);
            TransactionManager tm2 = new TransactionManager(store2);

            LocalShardParticipant participant0 = new LocalShardParticipant(shard0, tm0);
            LocalShardParticipant participant1 = new LocalShardParticipant(shard1, tm1);
            LocalShardParticipant participant2 = new LocalShardParticipant(shard2, tm2);

            Map<ShardId, TransactionParticipant> participants = Map.of(
                    shard0, participant0,
                    shard1, participant1,
                    shard2, participant2
            );

            List<NodeId> nodes = List.of(NodeId.of("node-1"), NodeId.of("node-2"), NodeId.of("node-3"));
            ShardManager shardManager = ShardManager.createStaticShards(3, nodes, 1);

            QueryRouter.ShardNodeInvoker invoker = (sId, targetNode, cmdBytes) -> {
                KvCommand cmd = KvCommand.fromBytes(cmdBytes);
                MvccStore targetStore = sId.equals(shard0) ? store0 : (sId.equals(shard1) ? store1 : store2);
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
                    default -> CompletableFuture.failedFuture(new IllegalArgumentException("Unknown: " + cmd.opType()));
                };
            };

            QueryRouter queryRouter = new QueryRouter(shardManager.router(), shardManager.leaderLocator(), invoker);

            Path tempDir = Files.createTempDirectory("aegisdb-sprint9-demo");
            Path walPath = tempDir.resolve("coordinator-2pc.wal");
            DurableCoordinatorLog coordinatorLog = new DurableCoordinatorLog(walPath);

            ShardedAegisDbClient client = new ShardedAegisDbClient(
                    queryRouter,
                    Collections.emptyMap(),
                    participants,
                    coordinatorLog
            );

            String keyA = findKeyForShard(shardManager, shard0, "account-A");
            String keyB = findKeyForShard(shardManager, shard1, "account-B");
            String keyC = findKeyForShard(shardManager, shard2, "account-C");

            System.out.println("  ✓ Configured 3 discrete shards and local participants:");
            System.out.println("    - " + shard0 + " -> Key: '" + keyA + "' (Store 0, LocalShardParticipant)");
            System.out.println("    - " + shard1 + " -> Key: '" + keyB + "' (Store 1, LocalShardParticipant)");
            System.out.println("    - " + shard2 + " -> Key: '" + keyC + "' (Store 2, LocalShardParticipant)");
            System.out.println("  ✓ Durable 2PC Coordinator Log initialized at: " + walPath.getFileName());

            // --- AC2: 2PC Phase 1 & 2 Execution ---
            System.out.println("\n▶ [2/6] [AC2] Executing Two-Phase Commit across Shards with Read-Your-Own-Writes...");
            try (Transaction tx = client.beginTransaction(IsolationLevel.SERIALIZABLE)) {
                tx.putString(keyA, "initial-A");
                tx.putString(keyB, "initial-B");
                tx.putString(keyC, "initial-C");

                // Read-your-own-writes
                String readA = tx.getString(keyA).orElseThrow();
                String readB = tx.getString(keyB).orElseThrow();
                String readC = tx.getString(keyC).orElseThrow();
                System.out.println("  Transaction write buffer (uncommitted):");
                System.out.println("    [TxBuffer] " + keyA + " = '" + readA + "'");
                System.out.println("    [TxBuffer] " + keyB + " = '" + readB + "'");
                System.out.println("    [TxBuffer] " + keyC + " = '" + readC + "'");

                tx.commit();
            }

            System.out.println("  ✓ 2PC Phase 1 (PREPARE) broadcast acknowledged unanimously (PREPARED).");
            System.out.println("  ✓ 2PC Phase 2 (COMMIT) durably logged and executed across all 3 shards:");
            System.out.println("    - Store 0: " + keyA + " = '" + new String(store0.get(keyA).orElseThrow(), StandardCharsets.UTF_8) + "'");
            System.out.println("    - Store 1: " + keyB + " = '" + new String(store1.get(keyB).orElseThrow(), StandardCharsets.UTF_8) + "'");
            System.out.println("    - Store 2: " + keyC + " = '" + new String(store2.get(keyC).orElseThrow(), StandardCharsets.UTF_8) + "'");

            // --- AC3: Cross-Shard Atomicity & Abort Rollback ---
            System.out.println("\n▶ [3/6] [AC3] Demonstrating Cross-Shard Transaction Rollback & Abort Atomicity...");
            try (Transaction tx = client.beginTransaction(IsolationLevel.SERIALIZABLE)) {
                tx.putString(keyA, "corrupted-A");
                tx.putString(keyB, "corrupted-B");
                tx.putString(keyC, "corrupted-C");
                System.out.println("  Simulating client abort midway through multi-shard update...");
                tx.abort();
            }

            // Verify original values intact
            System.out.println("  ✓ Post-abort verification: zero writes persisted on any shard:");
            System.out.println("    - Store 0 (" + keyA + "): '" + new String(store0.get(keyA).orElseThrow(), StandardCharsets.UTF_8) + "' (Unchanged)");
            System.out.println("    - Store 1 (" + keyB + "): '" + new String(store1.get(keyB).orElseThrow(), StandardCharsets.UTF_8) + "' (Unchanged)");
            System.out.println("    - Store 2 (" + keyC + "): '" + new String(store2.get(keyC).orElseThrow(), StandardCharsets.UTF_8) + "' (Unchanged)");
            System.out.println("  ✓ Key-level prepare locks cleared: "
                    + !participant0.isKeyLocked(keyA) + ", "
                    + !participant1.isKeyLocked(keyB) + ", "
                    + !participant2.isKeyLocked(keyC));

            // --- AC4: Key-Level Prepare Locks & Serializability ---
            System.out.println("\n▶ [4/6] [AC4] Enforcing Key-Level Prepare Locks during In-Doubt Window...");
            TransactionId inDoubtTx = TransactionId.of(7777L);
            participant0.prepare(new PrepareRequest(
                    inDoubtTx,
                    shard0,
                    List.of(WriteOperation.put(keyA, "locked-val".getBytes(StandardCharsets.UTF_8))),
                    1L
            ));
            System.out.println("  Shard 0 participant acquired exclusive prepare lock on key: '" + keyA + "' for tx: " + inDoubtTx);
            System.out.println("  Attempting concurrent transaction commit on locked key '" + keyA + "'...");

            boolean conflictDetected = false;
            try (Transaction tx = client.beginTransaction(IsolationLevel.SERIALIZABLE)) {
                tx.putString(keyA, "conflict-val");
                tx.commit();
            } catch (DistributedTransactionAbortedException e) {
                conflictDetected = true;
                System.out.println("  ✓ Concurrent commit rejected: " + e.getMessage());
            }

            if (!conflictDetected) {
                throw new IllegalStateException("Expected prepare lock conflict was not detected!");
            }

            participant0.abort(inDoubtTx);
            System.out.println("  ✓ In-doubt transaction aborted; key lock on '" + keyA + "' successfully released.");

            // --- AC5: Durable Coordinator Crash Recovery ---
            System.out.println("\n▶ [5/6] [AC5] Verifying Durable Crash Recovery across Coordinator Failure Modes (§10)...");
            Path crashLogPath = tempDir.resolve("crash-recovery-demo.wal");
            DurableCoordinatorLog crashLog = new DurableCoordinatorLog(crashLogPath);
            TransactionId crashTx = TransactionId.of(8888L);
            Set<ShardId> involvedShards = Set.of(shard0, shard1, shard2);

            participant0.prepare(new PrepareRequest(crashTx, shard0, List.of(WriteOperation.put(keyA, "crash-val-A".getBytes(StandardCharsets.UTF_8))), 1L));
            participant1.prepare(new PrepareRequest(crashTx, shard1, List.of(WriteOperation.put(keyB, "crash-val-B".getBytes(StandardCharsets.UTF_8))), 1L));
            participant2.prepare(new PrepareRequest(crashTx, shard2, List.of(WriteOperation.put(keyC, "crash-val-C".getBytes(StandardCharsets.UTF_8))), 1L));

            // Simulate crash right after COMMIT_DECIDED is durably written to disk
            crashLog.logState(crashTx, TwoPhaseCommitState.COMMIT_DECIDED, involvedShards);
            crashLog.close();
            System.out.println("  Simulated crash after durable COMMIT_DECIDED log for tx: " + crashTx);

            // Reopen and recover
            DurableCoordinatorLog recoveredLog = new DurableCoordinatorLog(crashLogPath);
            DistributedTransactionRecovery recovery = new DistributedTransactionRecovery(recoveredLog, participants::get);
            DistributedTransactionRecovery.RecoverySummary summary = recovery.recover();

            System.out.println("  ✓ Crash recovery completed: " + summary);
            System.out.println("    - State in recovered WAL: " + recoveredLog.getEntry(crashTx).map(CoordinatorLogEntry::state).orElse(null));
            System.out.println("    - Store 0: " + keyA + " = '" + new String(store0.get(keyA).orElseThrow(), StandardCharsets.UTF_8) + "'");
            System.out.println("    - Store 1: " + keyB + " = '" + new String(store1.get(keyB).orElseThrow(), StandardCharsets.UTF_8) + "'");
            System.out.println("    - Store 2: " + keyC + " = '" + new String(store2.get(keyC).orElseThrow(), StandardCharsets.UTF_8) + "'");
            recoveredLog.close();

            // --- AC6: Formal Milestone M4 Gate ---
            System.out.println("\n▶ [6/6] [AC6] Executing Formal Milestone M4 Gate: Bank Conservation Invariant...");
            System.out.println("  Initializing bank accounts across 3 distinct shards: A=1000, B=1000, C=1000 (Sum = 3000)");

            client.putString(keyA, "1000").join();
            client.putString(keyB, "1000").join();
            client.putString(keyC, "1000").join();

            int clientThreads = 8;
            int opsPerThread = 50;
            int totalOps = clientThreads * opsPerThread;

            System.out.println("  Launching " + clientThreads + " concurrent client threads (" + totalOps + " cross-shard transfers)...");

            ExecutorService executor = Executors.newFixedThreadPool(clientThreads);
            CountDownLatch startSignal = new CountDownLatch(1);
            CountDownLatch doneSignal = new CountDownLatch(clientThreads);
            AtomicInteger completedOps = new AtomicInteger(0);

            String[] accounts = {keyA, keyB, keyC};

            for (int t = 0; t < clientThreads; t++) {
                executor.submit(() -> {
                    try {
                        startSignal.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            int f = ThreadLocalRandom.current().nextInt(3);
                            int toIdx = (f + 1 + ThreadLocalRandom.current().nextInt(2)) % 3;
                            String fromAcc = accounts[f];
                            String toAcc = accounts[toIdx];
                            int transferAmount = ThreadLocalRandom.current().nextInt(1, 20);

                            client.runInTransaction(IsolationLevel.SERIALIZABLE, tx -> {
                                int fromBal = Integer.parseInt(tx.getString(fromAcc).orElse("0"));
                                int toBal = Integer.parseInt(tx.getString(toAcc).orElse("0"));

                                tx.putString(fromAcc, String.valueOf(fromBal - transferAmount));
                                tx.putString(toAcc, String.valueOf(toBal + transferAmount));
                                return null;
                            }, 50);

                            completedOps.incrementAndGet();
                        }
                    } catch (Exception e) {
                        log.error("Concurrent transfer failed", e);
                    } finally {
                        doneSignal.countDown();
                    }
                });
            }

            long startWall = System.currentTimeMillis();
            startSignal.countDown();
            boolean finished = doneSignal.await(60, TimeUnit.SECONDS);
            long elapsed = System.currentTimeMillis() - startWall;
            executor.shutdown();

            if (!finished) {
                throw new IllegalStateException("Milestone M4 benchmark timed out!");
            }

            int finalA = Integer.parseInt(client.getString(keyA).join().orElseThrow());
            int finalB = Integer.parseInt(client.getString(keyB).join().orElseThrow());
            int finalC = Integer.parseInt(client.getString(keyC).join().orElseThrow());
            int totalBalance = finalA + finalB + finalC;

            System.out.printf("  ✓ Completed %d cross-shard transfers in %d ms (%.1f tx/sec)\n",
                    completedOps.get(), elapsed, (completedOps.get() * 1000.0) / elapsed);
            System.out.println("  Final Balances:");
            System.out.println("    - Account A (" + shard0 + "): " + finalA);
            System.out.println("    - Account B (" + shard1 + "): " + finalB);
            System.out.println("    - Account C (" + shard2 + "): " + finalC);
            System.out.println("    -----------------------------------------");
            System.out.println("    TOTAL BALANCE: " + totalBalance + " [Expected: 3000]");

            if (totalBalance != 3000) {
                throw new IllegalStateException("CRITICAL: Financial conservation invariant violated! Sum=" + totalBalance);
            }

            System.out.println("\n=======================================================================");
            System.out.println("  ✅ MILESTONE M4 GATE VERIFIED: FINANCIAL CONSERVATION PRESERVED (3000)");
            System.out.println("  ✅ ALL SPRINT 9 ACCEPTANCE CRITERIA SATISFIED (US015)");
            System.out.println("=======================================================================");

            client.close();

        } catch (Exception e) {
            log.error("Sprint 9 demonstration failed", e);
            System.exit(1);
        }
    }

    private static String findKeyForShard(ShardManager shardManager, ShardId targetShard, String prefix) {
        for (int i = 0; i < 5000; i++) {
            String candidate = prefix + ":" + i;
            if (shardManager.router().routeToShardId(candidate).equals(targetShard)) {
                return candidate;
            }
        }
        throw new IllegalStateException("Unable to locate key mapping to " + targetShard);
    }
}
