package se.mouaz.aegisdb.transaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.mvcc.MvccStore;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for ConflictDetector under Snapshot Isolation and Serializable Isolation (Master Plan §9 & §18).
 */
class ConflictDetectorTest {

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
    @DisplayName("Snapshot Isolation: concurrent writes to different keys succeed without conflict")
    void disjointKeysDoNotConflict() {
        Transaction tx1 = manager.beginTransaction();
        Transaction tx2 = manager.beginTransaction();

        tx1.putString("x", "valX");
        tx2.putString("y", "valY");

        tx1.commit();
        tx2.commit();

        assertThat(store.get("x")).isPresent();
        assertThat(store.get("y")).isPresent();
    }

    @Test
    @DisplayName("Snapshot Isolation: concurrent write to same key throws WriteConflictException on second committer")
    void concurrentWriteConflictThrows() {
        Transaction tx1 = manager.beginTransaction();
        Transaction tx2 = manager.beginTransaction();

        tx1.putString("account_1", "100");
        assertThatThrownBy(() -> tx2.putString("account_1", "200"))
                .isInstanceOf(WriteConflictException.class)
                .hasMessageContaining("Write conflict on key 'account_1'");

        tx1.commit();
        tx2.abort();
    }

    @Test
    @DisplayName("Snapshot Isolation: write after concurrent commit throws WriteConflictException (First-Committer-Wins)")
    void firstCommitterWinsConflict() {
        store.put("shared_counter", "0".getBytes(StandardCharsets.UTF_8));

        Transaction tx1 = manager.beginTransaction();
        Transaction tx2 = manager.beginTransaction();

        // tx1 updates and commits first
        tx1.putString("shared_counter", "1");
        tx1.commit();

        // tx2 tries to write after tx1 already committed
        assertThatThrownBy(() -> tx2.putString("shared_counter", "2"))
                .isInstanceOf(WriteConflictException.class);

        tx2.abort();
    }

    @Test
    @DisplayName("Serializable Isolation: detecting read-set anti-dependency throws SerializationFailureException")
    void serializableReadSetConflict() {
        store.put("guard_flag", "true".getBytes());

        // Start serializable transaction that reads guard_flag
        Transaction tx1 = manager.beginTransaction(IsolationLevel.SERIALIZABLE);
        assertThat(tx1.getString("guard_flag")).contains("true");

        // Concurrent transaction mutates and commits guard_flag
        Transaction tx2 = manager.beginTransaction();
        tx2.putString("guard_flag", "false");
        tx2.commit();

        // tx1 tries to commit: should detect anti-dependency and fail
        tx1.putString("result", "done");
        assertThatThrownBy(tx1::commit)
                .isInstanceOf(SerializationFailureException.class)
                .hasMessageContaining("Serialization failure on key 'guard_flag'");

        tx1.abort();
    }
}
