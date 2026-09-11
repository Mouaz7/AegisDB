package se.mouaz.aegisdb.mvcc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Standard concurrency anomaly tests required by Master Project Plan §9:
 * <ul>
 *   <li>Dirty read</li>
 *   <li>Lost update</li>
 *   <li>Non-repeatable read</li>
 *   <li>Write-write conflict</li>
 *   <li>Write skew</li>
 *   <li>Concurrent account transfers</li>
 * </ul>
 */
@DisplayName("MVCC Concurrency Anomaly Tests (Master Plan §9)")
class MvccAnomalyTest {

    private MvccStore store;

    @BeforeEach
    void setUp() {
        store = new MvccStore();
    }

    @Test
    @DisplayName("Anomaly 1: Dirty Read is prevented — uncommitted writes are never visible to other readers")
    void dirtyReadPrevented() {
        store.put("key", "initial".getBytes(StandardCharsets.UTF_8));

        long tx1 = store.beginTransaction();
        store.put("key", "dirty-value".getBytes(StandardCharsets.UTF_8), tx1);

        // Transaction 2 reads concurrently
        long tx2 = store.beginTransaction();
        try (Snapshot snap2 = store.createSnapshotForTransaction(tx2)) {
            Optional<byte[]> val2 = store.get("key", snap2);
            assertThat(val2).isPresent();
            assertThat(new String(val2.get(), StandardCharsets.UTF_8)).isEqualTo("initial");
        }

        // Tx 1 aborts
        store.abort(tx1);

        // Reader after abort still reads "initial"
        assertThat(new String(store.get("key").get(), StandardCharsets.UTF_8)).isEqualTo("initial");
    }

    @Test
    @DisplayName("Anomaly 2: Lost Update is prevented — First-Committer-Wins aborts conflicting concurrent update")
    void lostUpdatePrevented() {
        store.put("counter", "10".getBytes(StandardCharsets.UTF_8));

        // Two transactions start concurrently from the same initial state
        long tx1 = store.beginTransaction();
        long tx2 = store.beginTransaction();

        try (Snapshot snap1 = store.createSnapshotForTransaction(tx1);
             Snapshot snap2 = store.createSnapshotForTransaction(tx2)) {

            int val1 = Integer.parseInt(new String(store.get("counter", snap1).get(), StandardCharsets.UTF_8));
            int val2 = Integer.parseInt(new String(store.get("counter", snap2).get(), StandardCharsets.UTF_8));

            assertThat(val1).isEqualTo(10);
            assertThat(val2).isEqualTo(10);

            // Tx 1 increments and commits
            store.put("counter", Integer.toString(val1 + 1).getBytes(StandardCharsets.UTF_8), tx1);
            store.commit(tx1);

            // Tx 2 attempts to increment based on its stale snapshot
            assertThatThrownBy(() ->
                    store.put("counter", Integer.toString(val2 + 1).getBytes(StandardCharsets.UTF_8), tx2)
            ).isInstanceOf(WriteConflictException.class);

            store.abort(tx2);
        }

        // Final value is 11, not overwritten by stale tx2 write
        assertThat(new String(store.get("counter").get(), StandardCharsets.UTF_8)).isEqualTo("11");
    }

    @Test
    @DisplayName("Anomaly 3: Non-Repeatable Read is prevented — snapshot guarantees repeatable reads")
    void nonRepeatableReadPrevented() {
        store.put("price", "100".getBytes(StandardCharsets.UTF_8));

        long readerTx = store.beginTransaction();
        try (Snapshot readerSnap = store.createSnapshotForTransaction(readerTx)) {
            // First read
            Optional<byte[]> r1 = store.get("price", readerSnap);
            assertThat(new String(r1.get(), StandardCharsets.UTF_8)).isEqualTo("100");

            // Concurrent writer modifies and commits
            store.put("price", "250".getBytes(StandardCharsets.UTF_8));

            // Second read using original snapshot sees the EXACT same value
            Optional<byte[]> r2 = store.get("price", readerSnap);
            assertThat(new String(r2.get(), StandardCharsets.UTF_8)).isEqualTo("100");
        }
    }

    @Test
    @DisplayName("Anomaly 4: Write-Write Conflict is detected when concurrent transactions modify the same key")
    void writeWriteConflictDetected() {
        long txA = store.beginTransaction();
        long txB = store.beginTransaction();

        store.put("resource", "data-A".getBytes(StandardCharsets.UTF_8), txA);

        assertThatThrownBy(() ->
                store.put("resource", "data-B".getBytes(StandardCharsets.UTF_8), txB)
        ).isInstanceOf(WriteConflictException.class);

        store.commit(txA);
        store.abort(txB);
    }

    @Test
    @DisplayName("Anomaly 5: Write Skew demonstration under Snapshot Isolation (Doctor On-Call dilemma)")
    void writeSkewDemonstration() {
        // Snapshot Isolation allows write skew when two transactions read overlapping sets and write disjoint keys.
        // Setup: At least one doctor must remain on call.
        // Alice and Bob are both on call.
        store.put("doctor:alice", "on_call".getBytes(StandardCharsets.UTF_8));
        store.put("doctor:bob", "on_call".getBytes(StandardCharsets.UTF_8));

        long txAlice = store.beginTransaction();
        long txBob = store.beginTransaction();

        try (Snapshot snapAlice = store.createSnapshotForTransaction(txAlice);
             Snapshot snapBob = store.createSnapshotForTransaction(txBob)) {

            // Alice checks if someone else is on call (sees Bob is on call)
            boolean bobOnCall = "on_call".equals(new String(store.get("doctor:bob", snapAlice).get(), StandardCharsets.UTF_8));
            // Bob checks if someone else is on call (sees Alice is on call)
            boolean aliceOnCall = "on_call".equals(new String(store.get("doctor:alice", snapBob).get(), StandardCharsets.UTF_8));

            assertThat(bobOnCall).isTrue();
            assertThat(aliceOnCall).isTrue();

            // Alice takes leave (writes to doctor:alice)
            store.put("doctor:alice", "off_call".getBytes(StandardCharsets.UTF_8), txAlice);
            store.commit(txAlice);

            // Bob takes leave (writes to doctor:bob - disjoint key, so under pure SI both commit)
            store.put("doctor:bob", "off_call".getBytes(StandardCharsets.UTF_8), txBob);
            store.commit(txBob);
        }

        // Under pure Snapshot Isolation, write skew occurs (both off call).
        // (Phase 7 will implement Serializable conflict validation to detect predicate write skews).
        assertThat(new String(store.get("doctor:alice").get(), StandardCharsets.UTF_8)).isEqualTo("off_call");
        assertThat(new String(store.get("doctor:bob").get(), StandardCharsets.UTF_8)).isEqualTo("off_call");
    }
}
