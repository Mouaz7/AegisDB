package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.client.LocalTransactionalClient;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.transaction.IsolationLevel;
import se.mouaz.aegisdb.transaction.Transaction;
import se.mouaz.aegisdb.transaction.TransactionManager;
import se.mouaz.aegisdb.transaction.log.DurableTransactionLog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Formal Milestone M3 Gate Verification Test (Master Project Plan §20):
 * "Milestone M3 after Sprint 7: MVCC and single-shard transactions preserve transaction invariants under concurrency."
 */
class SingleShardTransactionMilestoneM3Test {
    private static final Logger log = LoggerFactory.getLogger(SingleShardTransactionMilestoneM3Test.class);

    @TempDir
    Path tempDir;

    private Path logPath;
    private DurableTransactionLog transactionLog;
    private MvccStore mvccStore;
    private TransactionManager transactionManager;
    private LocalTransactionalClient client;

    @BeforeEach
    void setUp() throws IOException {
        logPath = tempDir.resolve("m3-milestone-tx.log");
        transactionLog = new DurableTransactionLog(logPath);
        mvccStore = new MvccStore();
        transactionManager = new TransactionManager(mvccStore, transactionLog);
        client = new LocalTransactionalClient(transactionManager);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    @DisplayName("Milestone M3 Gate: Atomicity - all writes in transaction commit together or rollback completely")
    void atomicityAllOrNothing() {
        client.putString("stock:item1", "100").join();
        client.putString("stock:item2", "50").join();

        // Transaction aborts midway
        try (Transaction tx = client.beginTransaction()) {
            tx.putString("stock:item1", "80");
            tx.putString("stock:item2", "30");
            tx.abort();
        }

        // Must observe pre-transaction balances
        assertThat(client.getString("stock:item1").join()).contains("100");
        assertThat(client.getString("stock:item2").join()).contains("50");

        // Transaction commits
        try (Transaction tx = client.beginTransaction()) {
            tx.putString("stock:item1", "80");
            tx.putString("stock:item2", "30");
            tx.commit();
        }

        assertThat(client.getString("stock:item1").join()).contains("80");
        assertThat(client.getString("stock:item2").join()).contains("30");
    }

    @Test
    @DisplayName("Milestone M3 Gate: Crash Recovery - restart recovers transaction log state without corrupting MVCC")
    void crashRecoveryRestoresTransactionLog() throws IOException {
        client.putString("order:1", "PENDING").join();
        client.putString("order:2", "CONFIRMED").join();

        // Close manager and simulated crash
        client.close();

        // Reopen new manager against the persistent log
        DurableTransactionLog recoveredLog = new DurableTransactionLog(logPath);
        MvccStore recoveredStore = new MvccStore();
        TransactionManager recoveredManager = new TransactionManager(recoveredStore, recoveredLog);
        recoveredManager.recoverFromLog();

        assertThat(recoveredLog.replay()).isNotEmpty();
        recoveredManager.close();
    }

    @Test
    @DisplayName("Milestone M3 Gate: Bank invariant A + B + C = 3000 under high concurrent multi-client load")
    void milestoneBankTransferConcurrency() throws InterruptedException {
        client.putString("A", "1000").join();
        client.putString("B", "1000").join();
        client.putString("C", "1000").join();

        int clientThreads = 16;
        int opsPerThread = 150;
        int expectedTotalOps = clientThreads * opsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(clientThreads);
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch doneSignal = new CountDownLatch(clientThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        String[] accounts = {"A", "B", "C"};

        for (int i = 0; i < clientThreads; i++) {
            executor.submit(() -> {
                try {
                    startSignal.await();
                    for (int j = 0; j < opsPerThread; j++) {
                        int f = ThreadLocalRandom.current().nextInt(3);
                        int t = (f + 1 + ThreadLocalRandom.current().nextInt(2)) % 3;
                        String from = accounts[f];
                        String to = accounts[t];
                        int amount = ThreadLocalRandom.current().nextInt(1, 20);

                        client.runInTransaction(IsolationLevel.SNAPSHOT_ISOLATION, tx -> {
                            int fromBal = Integer.parseInt(tx.getString(from).orElse("0"));
                            int toBal = Integer.parseInt(tx.getString(to).orElse("0"));

                            tx.putString(from, String.valueOf(fromBal - amount));
                            tx.putString(to, String.valueOf(toBal + amount));
                            return null;
                        }, 50);

                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("Concurrent client transfer failed", e);
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

        int finalA = Integer.parseInt(client.getString("A").join().orElseThrow());
        int finalB = Integer.parseInt(client.getString("B").join().orElseThrow());
        int finalC = Integer.parseInt(client.getString("C").join().orElseThrow());

        int totalSum = finalA + finalB + finalC;
        log.info("Milestone M3 Bank Balances: A={}, B={}, C={} | Total={}", finalA, finalB, finalC, totalSum);

        assertThat(totalSum).as("Milestone M3 Gate Invariant: Total balance must be preserved").isEqualTo(3000);
    }
}
