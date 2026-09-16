package se.mouaz.aegisdb.transaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.mvcc.MvccStore;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Complete anomaly test suite required by Master Project Plan §9:
 * 1. Dirty Read
 * 2. Lost Update
 * 3. Non-Repeatable Read
 * 4. Write-Write Conflict
 * 5. Write Skew (SI demonstration vs Serializable prevention)
 * 6. Concurrent Account Transfers
 */
class ConcurrencyAnomalyTest {

    private MvccStore store;
    private TransactionManager manager;

    @BeforeEach
    void setUp() {
        store = new MvccStore();
        manager = new TransactionManager(store);
    }

    @AfterEach
    void tearDown() {
        manager.close();
    }

    // -------------------------------------------------------------
    // 1. Dirty Read Prevention
    // -------------------------------------------------------------
    @Test
    @DisplayName("1. Dirty Read: readers never observe uncommitted modifications from other transactions")
    void dirtyReadPrevention() {
        store.put("salary:101", "50000".getBytes(StandardCharsets.UTF_8));

        Transaction tx1 = manager.beginTransaction();
        tx1.putString("salary:101", "90000"); // uncommitted mutation

        // Reader transaction starts concurrently
        Transaction readerTx = manager.beginTransaction();
        assertThat(readerTx.getString("salary:101")).contains("50000");

        // tx1 aborts
        tx1.abort();

        // Reader transaction must still read stable committed value
        assertThat(readerTx.getString("salary:101")).contains("50000");
        readerTx.commit();
    }

    // -------------------------------------------------------------
    // 2. Lost Update Prevention
    // -------------------------------------------------------------
    @Test
    @DisplayName("2. Lost Update: concurrent updates detect conflict via First-Committer-Wins")
    void lostUpdatePrevention() {
        store.put("counter", "100".getBytes(StandardCharsets.UTF_8));

        Transaction tx1 = manager.beginTransaction();
        Transaction tx2 = manager.beginTransaction();

        int val1 = Integer.parseInt(tx1.getString("counter").orElse("0"));
        int val2 = Integer.parseInt(tx2.getString("counter").orElse("0"));

        tx1.putString("counter", String.valueOf(val1 + 50));
        tx1.commit(); // committed to 150

        // tx2 tries to commit 125 based on stale read: must be rejected
        assertThatThrownBy(() -> {
            tx2.putString("counter", String.valueOf(val2 + 25));
            tx2.commit();
        }).isInstanceOf(WriteConflictException.class);
        tx2.abort();

        // After retry, reads 150 and adds 25 -> 175
        manager.runInTransaction(tx -> {
            int current = Integer.parseInt(tx.getString("counter").orElse("0"));
            tx.putString("counter", String.valueOf(current + 25));
            return null;
        });

        Transaction finalTx = manager.beginTransaction();
        assertThat(finalTx.getString("counter")).contains("175");
        finalTx.commit();
    }

    // -------------------------------------------------------------
    // 3. Non-Repeatable Read Prevention
    // -------------------------------------------------------------
    @Test
    @DisplayName("3. Non-Repeatable Read: reads within a snapshot remain strictly immutable despite concurrent commits")
    void nonRepeatableReadPrevention() {
        store.put("profile:status", "ACTIVE".getBytes(StandardCharsets.UTF_8));

        Transaction longReader = manager.beginTransaction();
        assertThat(longReader.getString("profile:status")).contains("ACTIVE");

        // Concurrent transaction mutates and commits
        Transaction writer = manager.beginTransaction();
        writer.putString("profile:status", "SUSPENDED");
        writer.commit();

        // longReader reads again: must observe original point-in-time value
        assertThat(longReader.getString("profile:status")).contains("ACTIVE");
        longReader.commit();

        // New transaction sees SUSPENDED
        Transaction newReader = manager.beginTransaction();
        assertThat(newReader.getString("profile:status")).contains("SUSPENDED");
        newReader.commit();
    }

    // -------------------------------------------------------------
    // 4. Write-Write Conflict Detection
    // -------------------------------------------------------------
    @Test
    @DisplayName("4. Write-Write Conflict: concurrent writes to the same key are caught immediately")
    void writeWriteConflictDetection() {
        Transaction tx1 = manager.beginTransaction();
        Transaction tx2 = manager.beginTransaction();

        tx1.putString("resource_lock", "held_by_tx1");

        assertThatThrownBy(() -> tx2.putString("resource_lock", "held_by_tx2"))
                .isInstanceOf(WriteConflictException.class);

        tx1.commit();
        tx2.abort();
    }

    // -------------------------------------------------------------
    // 5. Write Skew Demonstration (SI vs Serializable)
    // -------------------------------------------------------------
    @Test
    @DisplayName("5a. Write Skew: permitted under standard Snapshot Isolation on disjoint keys")
    void writeSkewOccursUnderSnapshotIsolation() {
        // Invariant: BalanceA + BalanceB >= 0
        store.put("acc:A", "100".getBytes(StandardCharsets.UTF_8));
        store.put("acc:B", "100".getBytes(StandardCharsets.UTF_8));

        // Tx1 reads both, sees sum=200, withdraws 150 from A -> A = -50
        Transaction tx1 = manager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION);
        int a1 = Integer.parseInt(tx1.getString("acc:A").orElse("0"));
        int b1 = Integer.parseInt(tx1.getString("acc:B").orElse("0"));
        if (a1 + b1 >= 150) {
            tx1.putString("acc:A", String.valueOf(a1 - 150));
        }

        // Tx2 concurrently reads both, sees sum=200, withdraws 150 from B -> B = -50
        Transaction tx2 = manager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION);
        int a2 = Integer.parseInt(tx2.getString("acc:A").orElse("0"));
        int b2 = Integer.parseInt(tx2.getString("acc:B").orElse("0"));
        if (a2 + b2 >= 150) {
            tx2.putString("acc:B", String.valueOf(b2 - 150));
        }

        // Disjoint writes: both commit under pure Snapshot Isolation
        tx1.commit();
        tx2.commit();

        int finalA = Integer.parseInt(new String(store.get("acc:A").orElseThrow(), StandardCharsets.UTF_8));
        int finalB = Integer.parseInt(new String(store.get("acc:B").orElseThrow(), StandardCharsets.UTF_8));

        // Write skew demonstrated: total balance is -100 < 0
        assertThat(finalA + finalB).isEqualTo(-100);
    }

    @Test
    @DisplayName("5b. Write Skew: detected and prevented under SERIALIZABLE via read-set validation")
    void writeSkewPreventedUnderSerializable() {
        // Reset balances
        store.put("s_acc:A", "100".getBytes(StandardCharsets.UTF_8));
        store.put("s_acc:B", "100".getBytes(StandardCharsets.UTF_8));

        // Tx1 starts as SERIALIZABLE
        Transaction tx1 = manager.beginTransaction(IsolationLevel.SERIALIZABLE);
        int a1 = Integer.parseInt(tx1.getString("s_acc:A").orElse("0"));
        int b1 = Integer.parseInt(tx1.getString("s_acc:B").orElse("0"));
        if (a1 + b1 >= 150) {
            tx1.putString("s_acc:A", String.valueOf(a1 - 150));
        }

        // Tx2 starts as SERIALIZABLE
        Transaction tx2 = manager.beginTransaction(IsolationLevel.SERIALIZABLE);
        int a2 = Integer.parseInt(tx2.getString("s_acc:A").orElse("0"));
        int b2 = Integer.parseInt(tx2.getString("s_acc:B").orElse("0"));
        if (a2 + b2 >= 150) {
            tx2.putString("s_acc:B", String.valueOf(b2 - 150));
        }

        // Tx1 commits first
        tx1.commit();

        // Tx2 attempts to commit: should detect that s_acc:A was modified after tx2 read it!
        assertThatThrownBy(tx2::commit)
                .isInstanceOf(SerializationFailureException.class)
                .hasMessageContaining("Serialization failure on key 's_acc:A'");
        tx2.abort();

        // Invariant holds: sum is still >= 0 (only Tx1 succeeded)
        int finalA = Integer.parseInt(new String(store.get("s_acc:A").orElseThrow(), StandardCharsets.UTF_8));
        int finalB = Integer.parseInt(new String(store.get("s_acc:B").orElseThrow(), StandardCharsets.UTF_8));
        assertThat(finalA + finalB).isEqualTo(50); // -50 + 100 = 50 >= 0
    }

    // -------------------------------------------------------------
    // 6. Concurrent Account Transfers
    // -------------------------------------------------------------
    @Test
    @DisplayName("6. Concurrent Account Transfers: atomic transfers preserve total balance under multi-threaded execution")
    void concurrentAccountTransfers() throws InterruptedException {
        store.put("acc1", "500".getBytes(StandardCharsets.UTF_8));
        store.put("acc2", "500".getBytes(StandardCharsets.UTF_8));

        int threadCount = 8;
        int transfersPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successfulTransfers = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            final boolean forward = (i % 2 == 0);
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < transfersPerThread; j++) {
                        String from = forward ? "acc1" : "acc2";
                        String to = forward ? "acc2" : "acc1";
                        boolean transferDone = false;
                        for (int attempt = 0; attempt < 50 && !transferDone; attempt++) {
                            try {
                                manager.runInTransaction(IsolationLevel.SNAPSHOT_ISOLATION, tx -> {
                                    int fromBal = Integer.parseInt(tx.getString(from).orElse("0"));
                                    int toBal = Integer.parseInt(tx.getString(to).orElse("0"));
                                    tx.putString(from, String.valueOf(fromBal - 10));
                                    tx.putString(to, String.valueOf(toBal + 10));
                                    return null;
                                }, 50);
                                transferDone = true;
                                successfulTransfers.incrementAndGet();
                            } catch (WriteConflictException | SerializationFailureException retryable) {
                                try {
                                    Thread.sleep(2 + (attempt % 5));
                                } catch (InterruptedException ie) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    // Log any unexpected non-retryable worker exception
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(successfulTransfers.get()).isEqualTo(threadCount * transfersPerThread);

        int finalAcc1 = Integer.parseInt(new String(store.get("acc1").orElseThrow(), StandardCharsets.UTF_8));
        int finalAcc2 = Integer.parseInt(new String(store.get("acc2").orElseThrow(), StandardCharsets.UTF_8));

        assertThat(finalAcc1 + finalAcc2).isEqualTo(1000);
    }
}
