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
 * State machine lifecycle and transition tests (Master Project Plan §9 & §18).
 * State Machine:
 * ACTIVE -> PREPARING -> PREPARED -> COMMITTED
 *       \-> ABORTED
 */
class TransactionStateMachineTest {

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
    @DisplayName("Transaction starts in ACTIVE state")
    void transactionStartsActive() {
        Transaction tx = manager.beginTransaction();
        assertThat(tx.state()).isEqualTo(TransactionState.ACTIVE);
        assertThat(tx.state().isTerminal()).isFalse();
        tx.abort();
    }

    @Test
    @DisplayName("Valid transition path: ACTIVE -> PREPARING -> PREPARED -> COMMITTED")
    void validFourPhaseTransition() {
        Transaction tx = manager.beginTransaction();
        tx.put("k1", "val1".getBytes(StandardCharsets.UTF_8));

        tx.prepare();
        assertThat(tx.state()).isEqualTo(TransactionState.PREPARED);

        long commitTs = tx.commit();
        assertThat(commitTs).isGreaterThan(0L);
        assertThat(tx.state()).isEqualTo(TransactionState.COMMITTED);
        assertThat(tx.state().isTerminal()).isTrue();
    }

    @Test
    @DisplayName("Valid fast-path transition: ACTIVE -> COMMITTED directly")
    void validFastPathCommit() {
        Transaction tx = manager.beginTransaction();
        tx.put("k1", "fast-commit".getBytes(StandardCharsets.UTF_8));

        long commitTs = tx.commit();
        assertThat(commitTs).isGreaterThan(0L);
        assertThat(tx.state()).isEqualTo(TransactionState.COMMITTED);
    }

    @Test
    @DisplayName("Valid abort path from ACTIVE: ACTIVE -> ABORTED")
    void validAbortFromActive() {
        Transaction tx = manager.beginTransaction();
        tx.put("k1", "val".getBytes(StandardCharsets.UTF_8));

        tx.abort();
        assertThat(tx.state()).isEqualTo(TransactionState.ABORTED);
        assertThat(tx.state().isTerminal()).isTrue();
    }

    @Test
    @DisplayName("Valid abort path from PREPARED: PREPARED -> ABORTED")
    void validAbortFromPrepared() {
        Transaction tx = manager.beginTransaction();
        tx.put("k1", "val".getBytes(StandardCharsets.UTF_8));
        tx.prepare();
        assertThat(tx.state()).isEqualTo(TransactionState.PREPARED);

        tx.abort();
        assertThat(tx.state()).isEqualTo(TransactionState.ABORTED);
    }

    @Test
    @DisplayName("Cannot mutate transaction after it is terminal")
    void cannotMutateAfterTerminal() {
        Transaction tx = manager.beginTransaction();
        tx.commit();

        assertThatThrownBy(() -> tx.put("k2", "v2".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> tx.get("k2"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Cannot abort a committed transaction")
    void cannotAbortCommittedTransaction() {
        Transaction tx = manager.beginTransaction();
        tx.commit();

        assertThatThrownBy(tx::abort)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot abort committed transaction");
    }

    @Test
    @DisplayName("Cannot commit an aborted transaction")
    void cannotCommitAbortedTransaction() {
        Transaction tx = manager.beginTransaction();
        tx.abort();

        assertThatThrownBy(tx::commit)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot commit aborted transaction");
    }

    @Test
    @DisplayName("Commit is idempotent: repeated commit returns identical timestamp")
    void commitIsIdempotent() {
        Transaction tx = manager.beginTransaction();
        tx.put("k1", "idempotent".getBytes(StandardCharsets.UTF_8));

        long ts1 = tx.commit();
        long ts2 = tx.commit();

        assertThat(ts1).isEqualTo(ts2);
        assertThat(tx.state()).isEqualTo(TransactionState.COMMITTED);
    }

    @Test
    @DisplayName("Abort is idempotent: repeated abort is safe no-op")
    void abortIsIdempotent() {
        Transaction tx = manager.beginTransaction();
        tx.abort();
        tx.abort(); // should not throw

        assertThat(tx.state()).isEqualTo(TransactionState.ABORTED);
    }
}
