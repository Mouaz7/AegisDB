package se.mouaz.aegisdb.transaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.mvcc.MvccStore;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for bounded transaction lifetime and automatic TTL timeout expiration (Master Project Plan §12).
 */
class TransactionTimeoutTest {

    private MvccStore store;
    private TransactionManager manager;

    @BeforeEach
    void setUp() {
        store = new MvccStore();
        TransactionConfig shortTtlConfig = new TransactionConfig(
                10_000,
                Duration.ofMillis(50), // 50ms TTL
                IsolationLevel.SNAPSHOT_ISOLATION,
                3
        );
        manager = new TransactionManager(store, manager != null ? manager.transactionLog() : new se.mouaz.aegisdb.transaction.log.InMemoryTransactionLog(), shortTtlConfig);
    }

    @AfterEach
    void tearDown() {
        manager.close();
    }

    @Test
    @DisplayName("Transaction exceeding TTL throws TransactionTimeoutException upon commit")
    void transactionTimesOutOnCommit() throws InterruptedException {
        Transaction tx = manager.beginTransaction();
        tx.putString("timed_key", "value");

        // Wait past TTL
        Thread.sleep(80);

        assertThatThrownBy(tx::commit)
                .isInstanceOf(TransactionTimeoutException.class)
                .hasMessageContaining("expired");

        assertThat(manager.registry().isAborted(tx.id())).isTrue();
    }

    @Test
    @DisplayName("Transaction exceeding TTL throws TransactionTimeoutException upon mutation")
    void transactionTimesOutOnMutation() throws InterruptedException {
        Transaction tx = manager.beginTransaction();

        // Wait past TTL
        Thread.sleep(80);

        assertThatThrownBy(() -> tx.putString("late_write", "value"))
                .isInstanceOf(TransactionTimeoutException.class);
    }

    @Test
    @DisplayName("Sweep expired transactions automatically cleans up and releases resources")
    void sweepExpiredTransactionsCleansUp() throws InterruptedException {
        Transaction tx1 = manager.beginTransaction();
        tx1.putString("resource_key", "held");

        Thread.sleep(80);

        int swept = manager.sweepExpiredTransactions();
        assertThat(swept).isEqualTo(1);
        assertThat(manager.registry().isAborted(tx1.id())).isTrue();

        // New transaction can now acquire the key without conflict
        Transaction tx2 = manager.beginTransaction();
        tx2.putString("resource_key", "new_owner");
        long ts = tx2.commit();
        assertThat(ts).isGreaterThan(0L);
    }
}
