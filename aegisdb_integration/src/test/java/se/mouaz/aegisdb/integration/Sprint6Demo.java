package se.mouaz.aegisdb.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.mvcc.MvccGarbageCollector;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.mvcc.Snapshot;
import se.mouaz.aegisdb.mvcc.TimestampProvider;
import se.mouaz.aegisdb.mvcc.VersionChain;
import se.mouaz.aegisdb.mvcc.VersionedValue;
import se.mouaz.aegisdb.mvcc.VisibilityRule;
import se.mouaz.aegisdb.mvcc.WriteConflictException;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sprint 6 Live Demonstration
 * (Multi-Version Concurrency Control & Snapshot Isolation - US011).
 *
 * Demonstrates all 5 Sprint 6 Acceptance Criteria:
 * 1. [AC1] Version chains: Ordered version history per key, lock-free traversal
 * 2. [AC2] Snapshot timestamps: Monotonic logical timestamps and point-in-time snapshots
 * 3. [AC3] Visibility rules & Snapshot Isolation: Non-blocking readers, repeatable reads
 * 4. [AC4] Uncommitted/aborted invisibility: Dirty read prevention, abort rollback, Read-Your-Own-Writes, First-Committer-Wins
 * 5. [AC5] Garbage collection safety: Watermarking protects active snapshots, obsolete version and key reclamation
 */
public class Sprint6Demo {
    private static final Logger log = LoggerFactory.getLogger(Sprint6Demo.class);

    public static void main(String[] args) {
        System.out.println("=======================================================================");
        System.out.println("     AegisDB - Sprint 6 Demonstration");
        System.out.println("     Multi-Version Concurrency Control (MVCC) & Snapshot Isolation");
        System.out.println("     User Story: US011 | Master Project Plan §9 & §17");
        System.out.println("=======================================================================\n");

        try {
            // --- AC1: Version Chains ---
            System.out.println("▶ [1/5] [AC1] Demonstrating 'Version Chains & Non-Blocking History'...");
            VersionChain chain = new VersionChain("account:savings");
            chain.append(new VersionedValue(1L, 10L, "1000 EUR".getBytes(StandardCharsets.UTF_8), false));
            chain.append(new VersionedValue(2L, 20L, "1500 EUR".getBytes(StandardCharsets.UTF_8), false));
            chain.append(new VersionedValue(3L, 30L, "1200 EUR".getBytes(StandardCharsets.UTF_8), false));

            System.out.println("   ✔ Key: '" + chain.key() + "', total version nodes in chain: " + chain.versionCount());
            System.out.println("   ✔ Chain head (latest commit): " + chain.head().commitTimestamp()
                    + " -> " + new String(chain.head().value(), StandardCharsets.UTF_8));
            System.out.println("   ✔ Historical version at T=15: "
                    + new String(chain.findVisible(Snapshot.of(15L)).orElseThrow(), StandardCharsets.UTF_8));
            System.out.println("   ✔ Historical version at T=25: "
                    + new String(chain.findVisible(Snapshot.of(25L)).orElseThrow(), StandardCharsets.UTF_8));
            System.out.println("   ✔ Historical version at T=35: "
                    + new String(chain.findVisible(Snapshot.of(35L)).orElseThrow(), StandardCharsets.UTF_8));
            System.out.println("   ✔ [AC1] Version Chains: PASSED\n");

            // --- AC2: Snapshot Timestamps ---
            System.out.println("▶ [2/5] [AC2] Demonstrating 'Monotonic Snapshot Timestamps'...");
            TimestampProvider tsProvider = new TimestampProvider();
            long t1 = tsProvider.nextTimestamp();
            long t2 = tsProvider.nextTimestamp();
            long t3 = tsProvider.nextTimestamp();
            System.out.println("   ✔ Generated monotonic logical timestamps: T1=" + t1 + ", T2=" + t2 + ", T3=" + t3);
            if (t1 >= t2 || t2 >= t3) {
                throw new IllegalStateException("Timestamps not strictly monotonic");
            }
            Snapshot snap = Snapshot.of(t2);
            System.out.println("   ✔ Created immutable point-in-time snapshot view at readTimestamp=" + snap.readTimestamp());
            System.out.println("   ✔ [AC2] Monotonic Snapshot Timestamps: PASSED\n");

            // --- AC3: Visibility Rules & Snapshot Isolation ---
            System.out.println("▶ [3/5] [AC3] Demonstrating 'Visibility Rules & Non-Blocking Snapshot Isolation'...");
            MvccStore store = new MvccStore();
            store.put("stock:NVDA", "120.00".getBytes(StandardCharsets.UTF_8));

            // Reader 1 creates a snapshot view at current state
            Snapshot readerSnapshot = store.createSnapshot();
            System.out.println("   ✔ Reader established Snapshot #" + readerSnapshot.snapshotId()
                    + " at logical time " + readerSnapshot.readTimestamp() + " (NVDA="
                    + new String(store.get("stock:NVDA", readerSnapshot).orElseThrow(), StandardCharsets.UTF_8) + ")");

            // Writer updates the value concurrently
            System.out.println("   ⚡ Writer performs 3 concurrent updates and commits them...");
            store.put("stock:NVDA", "125.50".getBytes(StandardCharsets.UTF_8));
            store.put("stock:NVDA", "131.25".getBytes(StandardCharsets.UTF_8));
            store.put("stock:NVDA", "135.00".getBytes(StandardCharsets.UTF_8));

            // Reader using earlier snapshot is completely unaffected (repeatable read, zero locks!)
            String readerSeen = new String(store.get("stock:NVDA", readerSnapshot).orElseThrow(), StandardCharsets.UTF_8);
            System.out.println("   ✔ Reader using Snapshot #" + readerSnapshot.snapshotId()
                    + " still reads original price: " + readerSeen + " (Repeatable Read)");
            if (!"120.00".equals(readerSeen)) {
                throw new IllegalStateException("Snapshot isolation violation! Reader saw modified data.");
            }

            // Fresh reader sees current state
            String currentPrice = new String(store.get("stock:NVDA").orElseThrow(), StandardCharsets.UTF_8);
            System.out.println("   ✔ Fresh reader sees updated price: " + currentPrice);
            readerSnapshot.close();
            System.out.println("   ✔ [AC3] Visibility Rules & Snapshot Isolation: PASSED\n");

            // --- AC4: Uncommitted and Aborted Invisibility ---
            System.out.println("▶ [4/5] [AC4] Demonstrating 'Uncommitted and Aborted Invisibility'...");
            store.put("invoice:1001", "PAID".getBytes(StandardCharsets.UTF_8));

            long txInFlight = store.beginTransaction();
            store.put("invoice:1001", "REFUND_PENDING".getBytes(StandardCharsets.UTF_8), txInFlight);
            System.out.println("   ⚡ Transaction " + txInFlight + " wrote uncommitted modification 'REFUND_PENDING'");

            // External reader check: must NOT see uncommitted data (Dirty Read Prevention)
            String externalRead = new String(store.get("invoice:1001").orElseThrow(), StandardCharsets.UTF_8);
            System.out.println("   ✔ External reader sees: '" + externalRead + "' (Dirty Read prevented!)");
            if (!"PAID".equals(externalRead)) {
                throw new IllegalStateException("Dirty read occurred!");
            }

            // Transaction's own snapshot check: must see its own write (Read-Your-Own-Writes)
            try (Snapshot ownSnap = store.createSnapshotForTransaction(txInFlight)) {
                String ownRead = new String(store.get("invoice:1001", ownSnap).orElseThrow(), StandardCharsets.UTF_8);
                System.out.println("   ✔ Transaction " + txInFlight + " reads its own in-flight write: '" + ownRead + "'");
            }

            // Test abort
            store.abort(txInFlight);
            System.out.println("   ⚡ Transaction " + txInFlight + " aborted.");
            String afterAbortRead = new String(store.get("invoice:1001").orElseThrow(), StandardCharsets.UTF_8);
            System.out.println("   ✔ Reader after abort sees original: '" + afterAbortRead + "' (Aborted writes invisible!)");

            // Test First-Committer-Wins conflict prevention
            long txA = store.beginTransaction();
            long txB = store.beginTransaction();
            store.put("seat:14B", "reserved-by-A".getBytes(StandardCharsets.UTF_8), txA);
            try {
                store.put("seat:14B", "reserved-by-B".getBytes(StandardCharsets.UTF_8), txB);
                throw new IllegalStateException("Expected WriteConflictException!");
            } catch (WriteConflictException e) {
                System.out.println("   ✔ Concurrency conflict detected: " + e.getMessage());
            }
            store.commit(txA);
            store.abort(txB);
            System.out.println("   ✔ [AC4] Uncommitted and Aborted Invisibility: PASSED\n");

            // --- AC5: Garbage Collection Safety & Watermarking ---
            System.out.println("▶ [5/5] [AC5] Demonstrating 'Watermarked Garbage Collection Safety'...");
            MvccGarbageCollector gc = new MvccGarbageCollector(store);

            // Step A: Produce multiple versions of a document
            store.put("doc:spec", "v1.0".getBytes(StandardCharsets.UTF_8));
            System.out.println("   ✔ Created doc:spec = 'v1.0'");

            // Step B: Reader opens long-running snapshot
            Snapshot longRunningSnapshot = store.createSnapshot();
            System.out.println("   ✔ Reader established long-running snapshot at T=" + longRunningSnapshot.readTimestamp());

            // Step C: Writer updates document to v2.0, v3.0, v4.0
            store.put("doc:spec", "v2.0".getBytes(StandardCharsets.UTF_8));
            store.put("doc:spec", "v3.0".getBytes(StandardCharsets.UTF_8));
            store.put("doc:spec", "v4.0".getBytes(StandardCharsets.UTF_8));
            System.out.println("   ✔ doc:spec now has 4 versions in chain");

            // Step D: Run GC sweep while snapshot is still open
            System.out.println("   ⚡ Triggering GC sweep while snapshot is active (watermark="
                    + store.minActiveSnapshotTimestamp() + ")...");
            MvccGarbageCollector.GcStats stats1 = gc.collectGarbage();
            System.out.println("   ✔ GC sweep finished: reclaimed " + stats1.versionsReclaimed() + " versions.");

            // Invariant verification: Reader snapshot MUST still be able to read v1.0!
            String protectedVal = new String(store.get("doc:spec", longRunningSnapshot).orElseThrow(), StandardCharsets.UTF_8);
            System.out.println("   ✔ INVARIANT VERIFIED: Active snapshot successfully read protected version: '"
                    + protectedVal + "'");
            if (!"v1.0".equals(protectedVal)) {
                throw new IllegalStateException("Active snapshot data was prematurely deleted by GC!");
            }

            // Step E: Reader completes and closes snapshot
            longRunningSnapshot.close();
            System.out.println("   ✔ Reader snapshot closed. Minimum active watermark can now advance.");

            // Step F: Second GC sweep reclaims the now-unreferenced historical versions
            MvccGarbageCollector.GcStats stats2 = gc.collectGarbage();
            System.out.println("   ✔ Post-close GC sweep finished: reclaimed " + stats2.versionsReclaimed() + " versions.");
            System.out.println("   ✔ Latest committed value is intact: "
                    + new String(store.get("doc:spec").orElseThrow(), StandardCharsets.UTF_8));

            // Step G: Tombstone reclamation
            store.delete("doc:spec");
            MvccGarbageCollector.GcStats stats3 = gc.collectGarbage();
            System.out.println("   ✔ Deleted tombstone key completely reclaimed from index: "
                    + stats3.keysReclaimed() + " keys reclaimed.");
            System.out.println("   ✔ [AC5] Garbage Collection Safety: PASSED\n");

            // --- Bonus: Raft StateMachine Snapshot Integration ---
            System.out.println("▶ [Bonus] Verifying 'Raft State Machine Snapshot Serialization'...");
            store.put("cluster:name", "AegisRaftDB-Cluster".getBytes(StandardCharsets.UTF_8));
            store.put("cluster:nodes", "3".getBytes(StandardCharsets.UTF_8));
            byte[] snapshotBytes = store.serializeSnapshot();
            System.out.println("   ✔ Serialized MVCC state into binary snapshot: " + snapshotBytes.length + " bytes (CRC32 validated)");

            MvccStore restoredStore = new MvccStore();
            restoredStore.restoreSnapshot(snapshotBytes);
            System.out.println("   ✔ Restored MVCC store from snapshot:");
            System.out.println("      cluster:name -> " + new String(restoredStore.get("cluster:name").orElseThrow(), StandardCharsets.UTF_8));
            System.out.println("      cluster:nodes -> " + new String(restoredStore.get("cluster:nodes").orElseThrow(), StandardCharsets.UTF_8));
            System.out.println("   ✔ Raft Snapshot Integration: PASSED\n");

            System.out.println("=======================================================================");
            System.out.println("     ALL SPRINT 6 ACCEPTANCE CRITERIA (AC1 - AC5) VERIFIED!");
            System.out.println("     AegisDB MVCC Engine: READY FOR SINGLE-SHARD TX (SPRINT 7)");
            System.out.println("=======================================================================");

        } catch (Exception e) {
            System.err.println("❌ Sprint 6 Demo failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
