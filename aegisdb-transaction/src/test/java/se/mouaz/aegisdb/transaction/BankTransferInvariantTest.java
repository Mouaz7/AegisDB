package se.mouaz.aegisdb.transaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.mvcc.MvccStore;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Bank Invariant Test per Master Project Plan §14 & §18:
 * "Initial: A = 1000, B = 1000, C = 1000.
 *  Run thousands of concurrent transfers.
 *  Required invariant after every completed experiment: A + B + C = 3000"
 */
class BankTransferInvariantTest {
    private static final Logger log = LoggerFactory.getLogger(BankTransferInvariantTest.class);

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

    @Test
    @DisplayName("Bank Transfer Invariant: A + B + C = 3000 under thousands of concurrent transfers (Master Plan §14)")
    void bankTransferInvariantPreserved() throws InterruptedException {
        // 1. Initial State: A=1000, B=1000, C=1000 (Total = 3000)
        store.put("A", "1000".getBytes(StandardCharsets.UTF_8));
        store.put("B", "1000".getBytes(StandardCharsets.UTF_8));
        store.put("C", "1000".getBytes(StandardCharsets.UTF_8));

        int threadCount = 16;
        int transfersPerThread = 200; // 3,200 total transfers
        int totalTransfers = threadCount * transfersPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startSignal = new CountDownLatch(1);
        CountDownLatch completionSignal = new CountDownLatch(threadCount);
        AtomicInteger completedCount = new AtomicInteger(0);

        String[] accounts = {"A", "B", "C"};

        long startTime = System.currentTimeMillis();

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startSignal.await();
                    for (int i = 0; i < transfersPerThread; i++) {
                        // Pick two distinct accounts at random
                        int fromIdx = ThreadLocalRandom.current().nextInt(3);
                        int toIdx = (fromIdx + 1 + ThreadLocalRandom.current().nextInt(2)) % 3;
                        String from = accounts[fromIdx];
                        String to = accounts[toIdx];
                        int amount = ThreadLocalRandom.current().nextInt(1, 15);

                        manager.runInTransaction(IsolationLevel.SNAPSHOT_ISOLATION, tx -> {
                            int fromBal = Integer.parseInt(tx.getString(from).orElse("0"));
                            int toBal = Integer.parseInt(tx.getString(to).orElse("0"));

                            tx.putString(from, String.valueOf(fromBal - amount));
                            tx.putString(to, String.valueOf(toBal + amount));
                            return null;
                        }, 50);

                        completedCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    log.error("Transfer worker error", e);
                } finally {
                    completionSignal.countDown();
                }
            });
        }

        startSignal.countDown();
        boolean finished = completionSignal.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        long durationMs = System.currentTimeMillis() - startTime;

        assertThat(finished).as("All transfer threads should complete within timeout").isTrue();
        assertThat(completedCount.get()).isEqualTo(totalTransfers);

        // 2. Read final committed balances
        int balanceA = Integer.parseInt(new String(store.get("A").orElseThrow(), StandardCharsets.UTF_8));
        int balanceB = Integer.parseInt(new String(store.get("B").orElseThrow(), StandardCharsets.UTF_8));
        int balanceC = Integer.parseInt(new String(store.get("C").orElseThrow(), StandardCharsets.UTF_8));

        int totalBalance = balanceA + balanceB + balanceC;

        log.info("Bank Invariant Test Finished in {} ms: A={}, B={}, C={} | Total={}",
                durationMs, balanceA, balanceB, balanceC, totalBalance);
        log.info("Metrics: Commits={}, WriteConflicts={}, Throughput={:.1f} tx/sec",
                manager.metrics().commitCount(),
                manager.metrics().writeConflictCount(),
                (totalTransfers * 1000.0) / Math.max(1, durationMs));

        // 3. Strict Master Plan §14 Invariant Check
        assertThat(totalBalance)
                .as("Strict Bank Invariant requirement (Master Plan §14): A + B + C must equal 3000")
                .isEqualTo(3000);
    }
}
