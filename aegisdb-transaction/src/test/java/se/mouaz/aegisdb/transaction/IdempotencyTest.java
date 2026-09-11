package se.mouaz.aegisdb.transaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClientId;
import se.mouaz.aegisdb.common.RequestId;
import se.mouaz.aegisdb.mvcc.MvccStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Idempotency and duplicate request suppression tests (Master Project Plan §10).
 * COMMIT, ABORT, retries, and client write requests must be safe when delivered more than once.
 */
class IdempotencyTest {

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
    @DisplayName("Duplicate commit requests return identical commit timestamp")
    void duplicateCommitIdempotency() {
        Transaction tx = manager.beginTransaction();
        tx.putString("dedup_key", "val1");

        long ts1 = tx.commit();
        long ts2 = manager.commit(tx.id());
        long ts3 = tx.commit();

        assertThat(ts1).isEqualTo(ts2);
        assertThat(ts2).isEqualTo(ts3);
        assertThat(manager.registry().isCommitted(tx.id())).isTrue();
    }

    @Test
    @DisplayName("Duplicate abort requests are safe and idempotent")
    void duplicateAbortIdempotency() {
        Transaction tx = manager.beginTransaction();
        tx.putString("dedup_abort", "val");

        tx.abort();
        manager.abort(tx.id());
        tx.abort();

        assertThat(manager.registry().isAborted(tx.id())).isTrue();
    }

    @Test
    @DisplayName("ClientId + RequestId suppresses duplicate transaction instantiation (Master Plan §10)")
    void clientRequestDeduplication() {
        ClientId client = ClientId.of("client-42");
        RequestId req = RequestId.of(1001);

        Transaction tx1 = manager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION, client, req);
        tx1.putString("item", "widget");

        // Client retry with exact same ClientId + RequestId
        Transaction tx2 = manager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION, client, req);

        // Must resolve to the identical transaction context
        assertThat(tx2.id()).isEqualTo(tx1.id());
        assertThat(tx2.getString("item")).contains("widget");

        tx1.commit();

        // After commit, duplicate beginTransaction with same requestId is rejected
        assertThatThrownBy(() -> manager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION, client, req))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already committed");
    }

    @Test
    @DisplayName("ClientId + RequestId rejects instantiation if previous request was aborted")
    void clientRequestDeduplicationAfterAbort() {
        ClientId client = ClientId.of("client-42");
        RequestId req = RequestId.of(1002);

        Transaction tx = manager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION, client, req);
        tx.abort();

        assertThatThrownBy(() -> manager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION, client, req))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already aborted");
    }
}
