package se.mouaz.aegisdb.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.mvcc.Snapshot;
import se.mouaz.aegisdb.mvcc.WriteConflictException;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Benchmark addressing Research Question 3 (Master Project Plan §20):
 * "How does transaction contention affect latency and abort rate when using MVCC?"
 *
 * Dimensions:
 * - Contention level: Low Contention (100 distinct keys) vs High Contention (5 hot bank accounts)
 * - Concurrent worker threads: 1, 4, 16
 * Invariant: Financial conservation strictly verified (Total balance remains unchanged).
 * Measures: Abort rate (%), Throughput (tx/sec), P50/P95/P99 latency (ms).
 */
public class RQ3ContentionBenchmark {
    private static final Logger log = LoggerFactory.getLogger(RQ3ContentionBenchmark.class);

    public static List<BenchmarkResult> runSuite(int totalTx, long seed) throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        // Low Contention (100 accounts, 4 threads)
        results.add(runContentionExperiment("RQ3_LowContention", 100, 4, totalTx, seed));
        // Moderate Contention (20 accounts, 8 threads)
        results.add(runContentionExperiment("RQ3_ModerateContention", 20, 8, totalTx, seed));
        // High Contention (5 hot accounts, 16 threads)
        results.add(runContentionExperiment("RQ3_HighContention", 5, 16, totalTx, seed));
        return results;
    }

    public static BenchmarkResult runContentionExperiment(String expName, int accountCount,
                                                          int threadCount, int totalTx,
                                                          long seed) throws Exception {
        log.info("Running {}: accounts={}, threads={}, totalTx={}", expName, accountCount, threadCount, totalTx);

        MvccStore mvccStore = new MvccStore();

        final long INITIAL_BALANCE = 10_000L;
        final long EXPECTED_TOTAL = accountCount * INITIAL_BALANCE;

        // Initialize accounts
        for (int i = 0; i < accountCount; i++) {
            mvccStore.put("acc:" + i, String.valueOf(INITIAL_BALANCE).getBytes(StandardCharsets.UTF_8));
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        int txPerThread = totalTx / threadCount;

        AtomicLong successTx = new AtomicLong(0);
        AtomicLong abortedTx = new AtomicLong(0);
        ConcurrentLinkedDeque<Long> latenciesNanos = new ConcurrentLinkedDeque<>();

        long startTime = System.nanoTime();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                Random rand = new Random(seed + threadId * 1000L);
                try {
                    for (int i = 0; i < txPerThread; i++) {
                        int from = rand.nextInt(accountCount);
                        int to = rand.nextInt(accountCount);
                        while (to == from && accountCount > 1) {
                            to = rand.nextInt(accountCount);
                        }

                        long opStart = System.nanoTime();
                        boolean committed = executeTransfer(mvccStore, "acc:" + from, "acc:" + to, 10L);
                        long opDuration = System.nanoTime() - opStart;

                        latenciesNanos.add(opDuration);
                        if (committed) {
                            successTx.incrementAndGet();
                        } else {
                            abortedTx.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long totalDuration = System.nanoTime() - startTime;
        double elapsedSec = totalDuration / 1_000_000_000.0;
        long totalAttempts = successTx.get() + abortedTx.get();
        double throughput = (totalAttempts / elapsedSec);
        double abortRate = (totalAttempts > 0) ? ((double) abortedTx.get() / totalAttempts) : 0.0;

        List<Long> latList = new ArrayList<>(latenciesNanos);
        double p50 = BenchmarkUtils.calculatePercentile(latList, 50.0);
        double p95 = BenchmarkUtils.calculatePercentile(latList, 95.0);
        double p99 = BenchmarkUtils.calculatePercentile(latList, 99.0);
        double max = BenchmarkUtils.calculatePercentile(latList, 100.0);

        // Verify financial conservation invariant
        long actualTotal = 0;
        try (Snapshot snap = mvccStore.createSnapshot()) {
            for (int i = 0; i < accountCount; i++) {
                byte[] val = mvccStore.get("acc:" + i, snap).orElse(null);
                if (val != null) {
                    actualTotal += Long.parseLong(new String(val, StandardCharsets.UTF_8));
                }
            }
        }

        if (actualTotal != EXPECTED_TOTAL) {
            throw new IllegalStateException("CRITICAL INVARIANT VIOLATION: Total balance mismatch! Expected=" +
                    EXPECTED_TOTAL + " but was=" + actualTotal);
        }

        log.info("Invariant verified for {}: Sum balances = {} (Delta=0)", expName, actualTotal);

        BenchmarkConfig config = BenchmarkConfig.builder(expName)
                .clusterSize(1)
                .clientCount(threadCount)
                .batchSize(1)
                .totalOperations(totalTx)
                .seed(seed)
                .workload("BANK_TRANSFER_CONTENTION_" + accountCount + "_ACCOUNTS")
                .failureScenario("NONE")
                .build();

        return new BenchmarkResult(
                config,
                totalAttempts,
                successTx.get(),
                abortedTx.get(),
                elapsedSec,
                throughput,
                p50,
                p95,
                p99,
                max,
                abortRate,
                0.0,
                BenchmarkUtils.captureMetadata(seed)
        );
    }

    private static boolean executeTransfer(MvccStore store, String fromKey, String toKey, long amount) {
        long txId = store.beginTransaction();
        try (Snapshot snap = store.createSnapshotForTransaction(txId)) {
            byte[] fromBytes = store.get(fromKey, snap).orElse(null);
            byte[] toBytes = store.get(toKey, snap).orElse(null);

            if (fromBytes == null || toBytes == null) {
                store.abort(txId);
                return false;
            }

            long fromBal = Long.parseLong(new String(fromBytes, StandardCharsets.UTF_8));
            long toBal = Long.parseLong(new String(toBytes, StandardCharsets.UTF_8));

            if (fromBal < amount) {
                store.abort(txId);
                return false;
            }

            store.put(fromKey, Long.toString(fromBal - amount).getBytes(StandardCharsets.UTF_8), txId);
            store.put(toKey, Long.toString(toBal + amount).getBytes(StandardCharsets.UTF_8), txId);

            store.commit(txId);
            return true;
        } catch (WriteConflictException conflict) {
            store.abort(txId);
            return false;
        } catch (Exception ex) {
            store.abort(txId);
            return false;
        }
    }
}
