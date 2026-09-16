package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.transaction.*;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Write Skew Anomaly & Serializable Anti-Dependency Isolation Verification")
class WriteSkewIsolationTest {

    private MvccStore mvccStore;
    private TransactionManager txManager;

    @BeforeEach
    void setUp() {
        mvccStore = new MvccStore();
        txManager = new TransactionManager(mvccStore);

        // Seed initial balances: Account A = 100, Account B = 100 (Total = 200, invariant: A + B >= 0)
        Transaction initTx = txManager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION);
        initTx.put("account:A", "100".getBytes(StandardCharsets.UTF_8));
        initTx.put("account:B", "100".getBytes(StandardCharsets.UTF_8));
        initTx.commit();
    }

    @AfterEach
    void tearDown() {
        if (txManager != null) {
            txManager.close();
        }
    }

    @Test
    @DisplayName("Snapshot Isolation admits Write Skew anomaly with disjoint write-sets")
    void snapshotIsolationAdmitsWriteSkew() {
        // Transaction 1 reads both accounts, deduces total is 200, withdraws 150 from Account A
        Transaction tx1 = txManager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION);
        int balA1 = Integer.parseInt(new String(tx1.get("account:A").orElseThrow(), StandardCharsets.UTF_8));
        int balB1 = Integer.parseInt(new String(tx1.get("account:B").orElseThrow(), StandardCharsets.UTF_8));
        assertThat(balA1 + balB1).isEqualTo(200);

        tx1.put("account:A", String.valueOf(balA1 - 150).getBytes(StandardCharsets.UTF_8));

        // Concurrent Transaction 2 reads both accounts, deduces total is 200, withdraws 150 from Account B
        Transaction tx2 = txManager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION);
        int balA2 = Integer.parseInt(new String(tx2.get("account:A").orElseThrow(), StandardCharsets.UTF_8));
        int balB2 = Integer.parseInt(new String(tx2.get("account:B").orElseThrow(), StandardCharsets.UTF_8));
        assertThat(balA2 + balB2).isEqualTo(200);

        tx2.put("account:B", String.valueOf(balB2 - 150).getBytes(StandardCharsets.UTF_8));

        // Both transactions commit because their write-sets ({A} and {B}) do not overlap
        tx1.commit();
        tx2.commit();

        // Read final committed state
        Transaction verifyTx = txManager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION);
        int finalA = Integer.parseInt(new String(verifyTx.get("account:A").orElseThrow(), StandardCharsets.UTF_8));
        int finalB = Integer.parseInt(new String(verifyTx.get("account:B").orElseThrow(), StandardCharsets.UTF_8));
        verifyTx.commit();

        // Verifies that SNAPSHOT_ISOLATION admits write skew: A = -50, B = -50, total = -100 (< 0)
        assertThat(finalA).isEqualTo(-50);
        assertThat(finalB).isEqualTo(-50);
        assertThat(finalA + finalB).isLessThan(0);
    }

    @Test
    @DisplayName("Serializable Isolation detects Read-Set Anti-Dependency and prevents Write Skew")
    void serializablePreventsWriteSkew() {
        // Transaction 1 (SERIALIZABLE) reads A and B, withdraws 150 from A
        Transaction tx1 = txManager.beginTransaction(IsolationLevel.SERIALIZABLE);
        int balA1 = Integer.parseInt(new String(tx1.get("account:A").orElseThrow(), StandardCharsets.UTF_8));
        int balB1 = Integer.parseInt(new String(tx1.get("account:B").orElseThrow(), StandardCharsets.UTF_8));
        assertThat(balA1 + balB1).isEqualTo(200);

        tx1.put("account:A", String.valueOf(balA1 - 150).getBytes(StandardCharsets.UTF_8));

        // Concurrent Transaction 2 (SERIALIZABLE) reads A and B, withdraws 150 from B
        Transaction tx2 = txManager.beginTransaction(IsolationLevel.SERIALIZABLE);
        int balA2 = Integer.parseInt(new String(tx2.get("account:A").orElseThrow(), StandardCharsets.UTF_8));
        int balB2 = Integer.parseInt(new String(tx2.get("account:B").orElseThrow(), StandardCharsets.UTF_8));
        assertThat(balA2 + balB2).isEqualTo(200);

        tx2.put("account:B", String.valueOf(balB2 - 150).getBytes(StandardCharsets.UTF_8));

        // Transaction 1 commits first and establishes a new committed version on Account A
        tx1.commit();

        // Transaction 2 attempts to commit: anti-dependency validation catches that Account A
        // in Tx2's read set was updated by Tx1. Tx2 must be aborted!
        assertThatThrownBy(tx2::commit)
                .isInstanceOf(SerializationFailureException.class)
                .hasMessageContaining("Serialization failure on key 'account:A'");

        // Verify final state: Account A = -50, Account B = 100, Total = 50 (>= 0). Invariant preserved!
        Transaction verifyTx = txManager.beginTransaction(IsolationLevel.SERIALIZABLE);
        int finalA = Integer.parseInt(new String(verifyTx.get("account:A").orElseThrow(), StandardCharsets.UTF_8));
        int finalB = Integer.parseInt(new String(verifyTx.get("account:B").orElseThrow(), StandardCharsets.UTF_8));
        verifyTx.commit();

        assertThat(finalA).isEqualTo(-50);
        assertThat(finalB).isEqualTo(100);
        assertThat(finalA + finalB).isGreaterThanOrEqualTo(0);
    }
}
