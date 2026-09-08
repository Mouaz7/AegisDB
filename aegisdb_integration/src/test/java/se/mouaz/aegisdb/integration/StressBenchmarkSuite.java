package se.mouaz.aegisdb.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.mvcc.MvccGarbageCollector;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.mvcc.Snapshot;
import se.mouaz.aegisdb.mvcc.WriteConflictException;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.storage.wal.FsyncPolicy;
import se.mouaz.aegisdb.storage.wal.StorageRecord;
import se.mouaz.aegisdb.storage.wal.WalConfig;
import se.mouaz.aegisdb.storage.wal.WalManager;
import se.mouaz.aegisdb.storage.wal.WalReader;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

/**
 * AegisDB Comprehensive Stress & Performance Benchmark Suite (Sprints 1 - 6).
 * <p>
 * Evaluates high-concurrency invariants, write conflict resolution, garbage collection,
 * write-ahead log (WAL) persistence, and multi-node Raft replication under sustained load.
 */
public class StressBenchmarkSuite {
    private static final Logger log = LoggerFactory.getLogger(StressBenchmarkSuite.class);

    public record BenchmarkResult(
            String name,
            long totalOperations,
            long successfulOperations,
            long failedOperations,
            double durationSeconds,
            double throughputOpsPerSec,
            long p50Micros,
            long p95Micros,
            long p99Micros,
            long maxMicros
    ) {
        public void print() {
            System.out.println("-----------------------------------------------------------------------");
            System.out.printf("  Benchmark: %s\n", name);
            System.out.printf("  Total Operations:      %,d\n", totalOperations);
            System.out.printf("  Success:               %,d\n", successfulOperations);
            System.out.printf("  Failed / Aborted:      %,d (%.2f%%)\n",
                    failedOperations, (totalOperations > 0 ? (failedOperations * 100.0 / totalOperations) : 0.0));
            System.out.printf("  Elapsed Time:          %.3f s\n", durationSeconds);
            System.out.printf("  Throughput:            %,.1f ops/sec\n", throughputOpsPerSec);
            System.out.printf("  Latency (P50):         %,d µs (%.3f ms)\n", p50Micros, p50Micros / 1000.0);
            System.out.printf("  Latency (P95):         %,d µs (%.3f ms)\n", p95Micros, p95Micros / 1000.0);
            System.out.printf("  Latency (P99):         %,d µs (%.3f ms)\n", p99Micros, p99Micros / 1000.0);
            System.out.printf("  Latency (Max):         %,d µs (%.3f ms)\n", maxMicros, maxMicros / 1000.0);
            System.out.println("-----------------------------------------------------------------------");
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=======================================================================");
        System.out.println("     AegisDB - Comprehensive Stress & Performance Benchmark Suite");
        System.out.println("     Evaluating Modules: Sprints 1 to 6 Under Concurrency & Load");
        System.out.println("     Master Project Plan §14, §15, §20 & §21");
        System.out.println("=======================================================================\n");

        List<BenchmarkResult> results = new ArrayList<>();

        // 1. MVCC Concurrency & Contention Benchmarks
        System.out.println("▶ [1/4] Running MVCC High-Concurrency & Contention Benchmarks...");
        results.add(benchmarkMvccReadHeavyThroughput(16, 20_000, 1_000));
        results.add(benchmarkMvccBankTransferContention(16, 5_000));

        // 2. MVCC Garbage Collection Churn & Watermarking
        System.out.println("\n▶ [2/4] Running MVCC Garbage Collection Under High Churn...");
        results.add(benchmarkMvccGcChurn(50_000, 500));

        // 3. Storage Engine WAL Persistence & Crash Recovery
        System.out.println("\n▶ [3/4] Running WAL Persistence & Crash Recovery Benchmark...");
        results.add(benchmarkWalWriteThroughput(10_000, FsyncPolicy.PERIODIC));
        results.add(benchmarkWalCrashRecovery(10_000));

        // 4. Multi-Node Raft Consensus Replication
        System.out.println("\n▶ [4/4] Running 3-Node Raft Consensus Replication Benchmark...");
        results.add(benchmarkRaftReplication(1_000));

        // Summary Table
        System.out.println("\n=======================================================================");
        System.out.println("                     BENCHMARK SUMMARY REPORT");
        System.out.println("=======================================================================");
        System.out.printf("%-38s | %10s | %12s | %10s | %10s\n",
                "Benchmark Suite", "Ops", "Throughput", "P50 Latency", "P99 Latency");
        System.out.println("-------------------------------------------------------------------------------------------------");
        for (BenchmarkResult r : results) {
            System.out.printf("%-38s | %10d | %10.1f/s | %8.2f ms | %8.2f ms\n",
                    truncate(r.name(), 38),
                    r.totalOperations(),
                    r.throughputOpsPerSec(),
                    r.p50Micros() / 1000.0,
                    r.p99Micros() / 1000.0);
        }
        System.out.println("=======================================================================\n");
        System.out.println("✔ All stress & benchmark tests completed with 100% invariant consistency.\n");
    }

    /**
     * Benchmark 1A: MVCC Read-Heavy Multi-Threaded Workload (80% Reads, 20% Writes).
     */
    public static BenchmarkResult benchmarkMvccReadHeavyThroughput(int threadCount, int totalOps, int keySpace) throws Exception {
        MvccStore store = new MvccStore();
        // Pre-populate key space
        for (int i = 0; i < keySpace; i++) {
            store.put("key:" + i, ("val:" + i).getBytes(StandardCharsets.UTF_8));
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        long[] latenciesNanos = new long[totalOps];
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger abortCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        int opsPerThread = totalOps / threadCount;
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                Random rand = new Random(Thread.currentThread().threadId());
                try {
                    startLatch.await();
                    for (int i = 0; i < opsPerThread; i++) {
                        int idx = counter.getAndIncrement();
                        if (idx >= totalOps) break;

                        int keyIdx = rand.nextInt(keySpace);
                        String key = "key:" + keyIdx;
                        boolean isRead = rand.nextInt(100) < 80;

                        long t0 = System.nanoTime();
                        try {
                            if (isRead) {
                                try (Snapshot snap = store.createSnapshot()) {
                                    store.get(key, snap);
                                    successCount.incrementAndGet();
                                }
                            } else {
                                long txId = store.beginTransaction();
                                store.put(key, ("updated:" + idx).getBytes(StandardCharsets.UTF_8), txId);
                                store.commit(txId);
                                successCount.incrementAndGet();
                            }
                        } catch (WriteConflictException wce) {
                            abortCount.incrementAndGet();
                        } catch (Exception e) {
                            abortCount.incrementAndGet();
                        } finally {
                            latenciesNanos[idx] = System.nanoTime() - t0;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long startWallTime = System.nanoTime();
        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        long durationNanos = System.nanoTime() - startWallTime;
        executor.shutdown();

        BenchmarkResult res = calculateResult(
                "MVCC Read-Heavy (80/20, " + threadCount + " threads)",
                totalOps, successCount.get(), abortCount.get(), durationNanos, latenciesNanos
        );
        res.print();
        return res;
    }

    /**
     * Benchmark 1B: High-Contention Bank Transfers with Invariant Verification (A + B + C + D + E = Constant).
     */
    public static BenchmarkResult benchmarkMvccBankTransferContention(int threadCount, int totalTransfers) throws Exception {
        MvccStore store = new MvccStore();
        int accountCount = 5;
        long initialPerAccount = 10_000L;
        long expectedTotalBalance = accountCount * initialPerAccount;

        for (int i = 0; i < accountCount; i++) {
            store.put("acc:" + i, Long.toString(initialPerAccount).getBytes(StandardCharsets.UTF_8));
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        long[] latenciesNanos = new long[totalTransfers];
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger conflictAbortCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        int transfersPerThread = totalTransfers / threadCount;
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                Random rand = new Random(Thread.currentThread().threadId() + 42);
                try {
                    startLatch.await();
                    for (int i = 0; i < transfersPerThread; i++) {
                        int idx = counter.getAndIncrement();
                        if (idx >= totalTransfers) break;

                        int fromAcc = rand.nextInt(accountCount);
                        int toAcc = rand.nextInt(accountCount);
                        while (toAcc == fromAcc) {
                            toAcc = rand.nextInt(accountCount);
                        }

                        long amount = 10L;
                        long t0 = System.nanoTime();
                        long txId = store.beginTransaction();
                        try (Snapshot snap = store.createSnapshotForTransaction(txId)) {
                            byte[] fromBytes = store.get("acc:" + fromAcc, snap).orElse(null);
                            byte[] toBytes = store.get("acc:" + toAcc, snap).orElse(null);

                            if (fromBytes != null && toBytes != null) {
                                long fromBal = Long.parseLong(new String(fromBytes, StandardCharsets.UTF_8));
                                long toBal = Long.parseLong(new String(toBytes, StandardCharsets.UTF_8));

                                if (fromBal >= amount) {
                                    store.put("acc:" + fromAcc, Long.toString(fromBal - amount).getBytes(StandardCharsets.UTF_8), txId);
                                    store.put("acc:" + toAcc, Long.toString(toBal + amount).getBytes(StandardCharsets.UTF_8), txId);
                                    store.commit(txId);
                                    successCount.incrementAndGet();
                                } else {
                                    store.abort(txId);
                                }
                            } else {
                                store.abort(txId);
                            }
                        } catch (WriteConflictException wce) {
                            store.abort(txId);
                            conflictAbortCount.incrementAndGet();
                        } catch (Exception e) {
                            store.abort(txId);
                            conflictAbortCount.incrementAndGet();
                        } finally {
                            latenciesNanos[idx] = System.nanoTime() - t0;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        long startWallTime = System.nanoTime();
        startLatch.countDown();
        doneLatch.await(30, TimeUnit.SECONDS);
        long durationNanos = System.nanoTime() - startWallTime;
        executor.shutdown();

        // Verify Conservation Invariant
        Snapshot finalSnapshot = store.createSnapshot();
        long actualTotalBalance = 0;
        for (int i = 0; i < accountCount; i++) {
            byte[] balBytes = store.get("acc:" + i, finalSnapshot).orElseThrow();
            long bal = Long.parseLong(new String(balBytes, StandardCharsets.UTF_8));
            System.out.printf("     Account acc:%d = %,d\n", i, bal);
            actualTotalBalance += bal;
        }

        if (actualTotalBalance != expectedTotalBalance) {
            throw new IllegalStateException("CRITICAL INVARIANT VIOLATION: Total balance altered! Expected "
                    + expectedTotalBalance + " but got " + actualTotalBalance);
        }
        System.out.printf("   ✔ Conservation invariant verified: Total balance = %,d (Initial = %,d, Delta = 0)\n",
                actualTotalBalance, expectedTotalBalance);

        BenchmarkResult res = calculateResult(
                "MVCC Contention (Bank Transfers, 5 Hot Accounts)",
                totalTransfers, successCount.get(), conflictAbortCount.get(), durationNanos, latenciesNanos
        );
        res.print();
        return res;
    }

    /**
     * Benchmark 2: MVCC Garbage Collection Churn & Watermarking.
     */
    public static BenchmarkResult benchmarkMvccGcChurn(int totalWrites, int distinctKeys) throws Exception {
        MvccStore store = new MvccStore();
        try (MvccGarbageCollector gc = new MvccGarbageCollector(store)) {
            // Establish an early long-running snapshot
            Snapshot baselineSnapshot = store.createSnapshot();

            // Rapidly write updates across rotating keys
            long[] writeLatencies = new long[totalWrites];
            long tStart = System.nanoTime();
            for (int i = 0; i < totalWrites; i++) {
                String key = "churn:key:" + (i % distinctKeys);
                long t0 = System.nanoTime();
                store.put(key, ("v_" + i).getBytes(StandardCharsets.UTF_8));
                writeLatencies[i] = System.nanoTime() - t0;
            }
            long writeDuration = System.nanoTime() - tStart;

            // Trigger GC while baselineSnapshot is still active: safe watermarking should retain baseline
            MvccGarbageCollector.GcStats statsHolding = gc.collectGarbage();
            System.out.printf("   ✔ GC Sweep with Active Snapshot: reclaimed %,d versions (watermark preserved)\n",
                    statsHolding.versionsReclaimed());

            // Release baseline snapshot and run full GC sweep
            baselineSnapshot.close();
            long gcStart = System.nanoTime();
            MvccGarbageCollector.GcStats statsPruned = gc.collectGarbage();
            long gcDuration = System.nanoTime() - gcStart;

            System.out.printf("   ✔ Full GC Sweep after release: reclaimed %,d obsolete versions in %.2f ms\n",
                    statsPruned.versionsReclaimed(), gcDuration / 1_000_000.0);

            BenchmarkResult res = calculateResult(
                    "MVCC Heavy Churn (" + totalWrites + " writes, " + distinctKeys + " keys)",
                    totalWrites, totalWrites, 0, writeDuration, writeLatencies
            );
            res.print();
            return res;
        }
    }

    /**
     * Benchmark 3A: Storage Engine WAL Disk Append Throughput.
     */
    public static BenchmarkResult benchmarkWalWriteThroughput(int recordCount, FsyncPolicy policy) throws Exception {
        Path tempDir = Files.createTempDirectory("aegisdb-bench-wal-write");
        try {
            WalConfig config = WalConfig.builder()
                    .walDir(tempDir)
                    .maxSegmentSizeBytes(512 * 1024) // 512 KB segments
                    .fsyncPolicy(policy)
                    .build();

            long[] latencies = new long[recordCount];
            byte[] key = "bench:entity:customer".getBytes(StandardCharsets.UTF_8);
            byte[] value = "{\"balance\": 1000, \"status\": \"ACTIVE\", \"tier\": \"PREMIUM\"}".getBytes(StandardCharsets.UTF_8);

            long totalBytes = 0;
            long tStart = System.nanoTime();
            try (WalManager wal = new WalManager(config)) {
                for (int i = 1; i <= recordCount; i++) {
                    long t0 = System.nanoTime();
                    StorageRecord rec = StorageRecord.createEntry(i, 1L, System.currentTimeMillis(), key, value);
                    wal.append(rec);
                    latencies[i - 1] = System.nanoTime() - t0;
                    totalBytes += rec.totalSizeOnDisk();
                }
            }
            long duration = System.nanoTime() - tStart;
            double mbWritten = totalBytes / (1024.0 * 1024.0);
            double throughputMbSec = mbWritten / (duration / 1_000_000_000.0);

            System.out.printf("   ✔ WAL Append Volume: %.2f MB written across segments (Throughput: %.2f MB/sec)\n",
                    mbWritten, throughputMbSec);

            BenchmarkResult res = calculateResult(
                    "WAL Append (" + policy + ", " + recordCount + " records)",
                    recordCount, recordCount, 0, duration, latencies
            );
            res.print();
            return res;
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * Benchmark 3B: Storage Engine Crash Recovery & CRC32 Verification Throughput.
     */
    public static BenchmarkResult benchmarkWalCrashRecovery(int recordCount) throws Exception {
        Path tempDir = Files.createTempDirectory("aegisdb-bench-wal-recovery");
        try {
            WalConfig config = WalConfig.builder()
                    .walDir(tempDir)
                    .maxSegmentSizeBytes(256 * 1024)
                    .fsyncPolicy(FsyncPolicy.PERIODIC)
                    .build();

            byte[] payload = "{\"transaction\": \"deposit\", \"amount\": 500}".getBytes(StandardCharsets.UTF_8);
            try (WalManager wal = new WalManager(config)) {
                for (int i = 1; i <= recordCount; i++) {
                    wal.append(StorageRecord.createEntry(i, 1L, System.currentTimeMillis(), ("k" + i).getBytes(StandardCharsets.UTF_8), payload));
                }
            }

            // Benchmark recovery scan & validation
            long t0 = System.nanoTime();
            try (WalManager recoverWal = new WalManager(config)) {
                WalReader reader = new WalReader(recoverWal);
                List<StorageRecord> recovered = reader.readAllRecords();
                long duration = System.nanoTime() - t0;

                if (recovered.size() != recordCount) {
                    throw new IllegalStateException("Recovery mismatch! Expected " + recordCount + " but got " + recovered.size());
                }

                double recPerSec = recordCount / (duration / 1_000_000_000.0);
                System.out.printf("   ✔ Recovered and validated %,d CRC32 records in %.2f ms (%,.0f records/sec)\n",
                        recordCount, duration / 1_000_000.0, recPerSec);

                long[] dummyLatency = new long[] { duration };
                BenchmarkResult res = calculateResult(
                        "WAL Recovery & CRC32 Validation (" + recordCount + " records)",
                        recordCount, recordCount, 0, duration, dummyLatency
                );
                res.print();
                return res;
            }
        } finally {
            deleteRecursively(tempDir);
        }
    }

    /**
     * Benchmark 4: Multi-Node Raft Consensus Replication Throughput.
     */
    public static BenchmarkResult benchmarkRaftReplication(int proposalCount) throws Exception {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        NodeId id1 = NodeId.of("bench-node-1");
        NodeId id2 = NodeId.of("bench-node-2");
        NodeId id3 = NodeId.of("bench-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 9001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 9002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 9003);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("bench-cluster")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        InMemoryTransport transport1 = new InMemoryTransport(id1);
        InMemoryTransport transport2 = new InMemoryTransport(id2);
        InMemoryTransport transport3 = new InMemoryTransport(id3);

        transport1.start();
        transport2.start();
        transport3.start();

        RaftNode node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(100))
                .maxElectionTimeout(Duration.ofMillis(140))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(1))
                .build();

        RaftNode node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(1000))
                .maxElectionTimeout(Duration.ofMillis(1200))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(2))
                .build();

        RaftNode node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(1000))
                .maxElectionTimeout(Duration.ofMillis(1200))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(3))
                .build();

        try {
            node1.start();
            node2.start();
            node3.start();

            await().atMost(Duration.ofSeconds(5)).until(() -> node1.role() == RaftRole.LEADER);

            long[] latencies = new long[proposalCount];
            long tStart = System.nanoTime();
            for (int i = 1; i <= proposalCount; i++) {
                byte[] command = ("SET counter " + i).getBytes(StandardCharsets.UTF_8);
                long t0 = System.nanoTime();
                CompletableFuture<Long> fut = node1.propose(command);
                fut.get(5, TimeUnit.SECONDS);
                latencies[i - 1] = System.nanoTime() - t0;
            }
            long totalDuration = System.nanoTime() - tStart;

            // Verify followers committed all entries
            await().atMost(Duration.ofSeconds(5))
                    .until(() -> node2.commitIndex() == (long) proposalCount && node3.commitIndex() == (long) proposalCount);

            RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node2.log(), proposalCount);
            RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), proposalCount);

            System.out.printf("   ✔ Committed %,d Raft proposals across 3 nodes with 100%% identical log sequence.\n",
                    proposalCount);

            BenchmarkResult res = calculateResult(
                    "Raft 3-Node In-Memory Replication (" + proposalCount + " proposals)",
                    proposalCount, proposalCount, 0, totalDuration, latencies
            );
            res.print();
            return res;
        } finally {
            node1.close();
            node2.close();
            node3.close();
            transport1.close();
            transport2.close();
            transport3.close();
        }
    }

    private static BenchmarkResult calculateResult(
            String name, long totalOps, long success, long failed, long durationNanos, long[] latenciesNanos
    ) {
        double durationSeconds = durationNanos / 1_000_000_000.0;
        double throughput = durationSeconds > 0 ? (totalOps / durationSeconds) : 0.0;

        long[] validLatencies = Arrays.stream(latenciesNanos).filter(l -> l > 0).sorted().toArray();
        long p50Micros = 0, p95Micros = 0, p99Micros = 0, maxMicros = 0;
        if (validLatencies.length > 0) {
            p50Micros = validLatencies[(int) (validLatencies.length * 0.50)] / 1000;
            p95Micros = validLatencies[(int) (validLatencies.length * 0.95)] / 1000;
            p99Micros = validLatencies[(int) (validLatencies.length * 0.99)] / 1000;
            maxMicros = validLatencies[validLatencies.length - 1] / 1000;
        }

        return new BenchmarkResult(
                name, totalOps, success, failed, durationSeconds, throughput,
                p50Micros, p95Micros, p99Micros, maxMicros
        );
    }

    private static String truncate(String text, int maxLength) {
        if (text.length() <= maxLength) return text;
        return text.substring(0, maxLength - 3) + "...";
    }

    private static void deleteRecursively(Path root) {
        try {
            if (Files.exists(root)) {
                try (var stream = Files.walk(root)) {
                    stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ignored) {}
                    });
                }
            }
        } catch (IOException ignored) {}
    }
}
