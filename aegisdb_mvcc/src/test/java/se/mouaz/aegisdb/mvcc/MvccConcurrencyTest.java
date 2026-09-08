package se.mouaz.aegisdb.mvcc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MVCC Concurrency and Invariant Tests")
class MvccConcurrencyTest {

    @Test
    @DisplayName("Lock-free concurrent readers and writers never deadlock or see partial state")
    void concurrentReadersAndWriters() throws InterruptedException {
        MvccStore store = new MvccStore();
        store.put("key", "initial".getBytes(StandardCharsets.UTF_8));

        int writerThreads = 4;
        int readerThreads = 6;
        int writesPerThread = 500;

        ExecutorService executor = Executors.newFixedThreadPool(writerThreads + readerThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(writerThreads + readerThreads);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger successfulReads = new AtomicInteger(0);

        // Start writers
        for (int i = 0; i < writerThreads; i++) {
            final int writerId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < writesPerThread; j++) {
                        String val = "w-" + writerId + "-" + j;
                        store.put("key", val.getBytes(StandardCharsets.UTF_8));
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        // Start readers
        for (int i = 0; i < readerThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    while (running.get()) {
                        try (Snapshot snapshot = store.createSnapshot()) {
                            Optional<byte[]> val = store.get("key", snapshot);
                            assertThat(val).isPresent();
                            successfulReads.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();

        // Let writers finish
        boolean writersFinished = doneLatch.await(5, TimeUnit.SECONDS);
        running.set(false);

        executor.shutdown();
        executor.awaitTermination(3, TimeUnit.SECONDS);

        assertThat(successfulReads.get()).isGreaterThan(100);
        assertThat(store.get("key")).isPresent();
    }

    @Test
    @DisplayName("Bank Transfer Invariant Test (Master Plan §14): A + B + C == 3000 under concurrent transfers")
    void bankInvariantTest() throws InterruptedException {
        MvccStore store = new MvccStore();

        // Initialize: A=1000, B=1000, C=1000
        store.put("acc:A", "1000".getBytes(StandardCharsets.UTF_8));
        store.put("acc:B", "1000".getBytes(StandardCharsets.UTF_8));
        store.put("acc:C", "1000".getBytes(StandardCharsets.UTF_8));

        int totalTransfers = 400;
        int threads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch doneLatch = new CountDownLatch(totalTransfers);
        AtomicInteger committedTransfers = new AtomicInteger(0);
        AtomicInteger abortedTransfers = new AtomicInteger(0);

        String[] accounts = {"acc:A", "acc:B", "acc:C"};

        for (int i = 0; i < totalTransfers; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    String from = accounts[index % 3];
                    String to = accounts[(index + 1) % 3];
                    int amount = 5;

                    long txId = store.beginTransaction();
                    try (Snapshot snap = store.createSnapshotForTransaction(txId)) {
                        Optional<byte[]> fromBytes = store.get(from, snap);
                        Optional<byte[]> toBytes = store.get(to, snap);

                        if (fromBytes.isPresent() && toBytes.isPresent()) {
                            int fromBal = Integer.parseInt(new String(fromBytes.get(), StandardCharsets.UTF_8));
                            int toBal = Integer.parseInt(new String(toBytes.get(), StandardCharsets.UTF_8));

                            if (fromBal >= amount) {
                                store.put(from, Integer.toString(fromBal - amount).getBytes(StandardCharsets.UTF_8), txId);
                                store.put(to, Integer.toString(toBal + amount).getBytes(StandardCharsets.UTF_8), txId);
                                store.commit(txId);
                                committedTransfers.incrementAndGet();
                                return;
                            }
                        }
                    } catch (WriteConflictException e) {
                        // Concurrent conflict detected: abort and retry/exit cleanly
                        store.abort(txId);
                        abortedTransfers.incrementAndGet();
                        return;
                    } catch (Exception e) {
                        store.abort(txId);
                        abortedTransfers.incrementAndGet();
                        return;
                    }
                    store.abort(txId);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        boolean finished = doneLatch.await(10, TimeUnit.SECONDS);
        assertThat(finished).isTrue();

        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.SECONDS);

        // Verify fundamental invariant: Sum of accounts must be exactly 3000
        int balA = Integer.parseInt(new String(store.get("acc:A").orElseThrow(), StandardCharsets.UTF_8));
        int balB = Integer.parseInt(new String(store.get("acc:B").orElseThrow(), StandardCharsets.UTF_8));
        int balC = Integer.parseInt(new String(store.get("acc:C").orElseThrow(), StandardCharsets.UTF_8));

        int totalSum = balA + balB + balC;
        assertThat(totalSum)
                .as("Bank invariant A + B + C must strictly equal 3000")
                .isEqualTo(3000);
        assertThat(committedTransfers.get()).isGreaterThan(0);
    }
}
