package se.mouaz.aegisdb.transaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.mvcc.MvccStore;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for ReadSet, WriteSet, Read-Your-Own-Writes, and resource bounds (Master Project Plan §9 & §12).
 */
class TransactionReadWriteSetTest {

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
    @DisplayName("Read-Your-Own-Writes: uncommitted put is immediately visible to own transaction")
    void readYourOwnWritesPut() {
        Transaction tx = manager.beginTransaction();
        tx.putString("alpha", "secret_v1");

        assertThat(tx.getString("alpha")).contains("secret_v1");

        // Concurrent transaction must NOT see it (Dirty read prevention)
        Transaction tx2 = manager.beginTransaction();
        assertThat(tx2.getString("alpha")).isEmpty();

        tx.commit();
        tx2.abort();
    }

    @Test
    @DisplayName("Read-Your-Own-Writes: uncommitted delete immediately shadows prior committed value")
    void readYourOwnWritesDelete() {
        store.put("beta", "initial_val".getBytes(StandardCharsets.UTF_8));

        Transaction tx = manager.beginTransaction();
        assertThat(tx.getString("beta")).contains("initial_val");

        tx.delete("beta");
        assertThat(tx.getString("beta")).isEmpty();

        tx.commit();

        Transaction txAfter = manager.beginTransaction();
        assertThat(txAfter.getString("beta")).isEmpty();
        txAfter.abort();
    }

    @Test
    @DisplayName("ReadSet records all accessed keys with their observed commit timestamps")
    void readSetTracking() {
        store.put("keyA", "valA".getBytes(StandardCharsets.UTF_8));

        Transaction tx = manager.beginTransaction();
        tx.get("keyA");
        tx.get("keyNonExistent");

        TransactionImpl impl = (TransactionImpl) tx;
        ReadSet rs = impl.context().readSet();

        assertThat(rs.size()).isEqualTo(2);
        assertThat(rs.contains("keyA")).isTrue();
        assertThat(rs.contains("keyNonExistent")).isTrue();
        assertThat(rs.getObservedTimestamp("keyA")).isGreaterThan(0L);
        assertThat(rs.getObservedTimestamp("keyNonExistent")).isEqualTo(0L);

        tx.abort();
    }

    @Test
    @DisplayName("WriteSet size is strictly bounded by maxWriteSetSize (Master Plan §12)")
    void writeSetCapacityLimit() {
        TransactionConfig boundedConfig = new TransactionConfig(
                3, // max 3 keys
                Duration.ofSeconds(30),
                IsolationLevel.SNAPSHOT_ISOLATION,
                3
        );
        TransactionManager boundedManager = new TransactionManager(store, manager.transactionLog(), boundedConfig);

        Transaction tx = boundedManager.beginTransaction();
        tx.putString("k1", "v1");
        tx.putString("k2", "v2");
        tx.putString("k3", "v3");

        // Overwrite to existing key is allowed
        tx.putString("k1", "v1_updated");

        // Fourth new key must trigger TransactionSizeLimitException
        assertThatThrownBy(() -> tx.putString("k4", "v4"))
                .isInstanceOf(TransactionSizeLimitException.class)
                .hasMessageContaining("exceeded maximum size limit of 3 keys");

        tx.abort();
        boundedManager.close();
    }
}
