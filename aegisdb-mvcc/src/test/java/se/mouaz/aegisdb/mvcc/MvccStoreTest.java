package se.mouaz.aegisdb.mvcc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("MVCC Store Tests")
class MvccStoreTest {

    private MvccStore store;

    @BeforeEach
    void setUp() {
        store = new MvccStore();
    }

    @Test
    @DisplayName("Single-operation auto-commit put and get")
    void autoCommitPutAndGet() {
        store.put("greeting", "hello world".getBytes(StandardCharsets.UTF_8));

        Optional<byte[]> result = store.get("greeting");
        assertThat(result).isPresent();
        assertThat(new String(result.get(), StandardCharsets.UTF_8)).isEqualTo("hello world");
    }

    @Test
    @DisplayName("Snapshot Isolation: readers see immutable point-in-time state despite subsequent writes")
    void snapshotIsolationPreserved() {
        store.put("account:1", "100".getBytes(StandardCharsets.UTF_8));

        // Reader takes snapshot
        Snapshot snapshot = store.createSnapshot();

        // Writer modifies and commits new balance
        store.put("account:1", "500".getBytes(StandardCharsets.UTF_8));

        // Reader using earlier snapshot still reads 100
        Optional<byte[]> readerVal = store.get("account:1", snapshot);
        assertThat(readerVal).isPresent();
        assertThat(new String(readerVal.get(), StandardCharsets.UTF_8)).isEqualTo("100");

        // Fresh read sees 500
        Optional<byte[]> freshVal = store.get("account:1");
        assertThat(freshVal).isPresent();
        assertThat(new String(freshVal.get(), StandardCharsets.UTF_8)).isEqualTo("500");

        snapshot.close();
    }

    @Test
    @DisplayName("Uncommitted writes from other transactions are invisible (dirty read prevention)")
    void dirtyReadPrevention() {
        store.put("status", "initial".getBytes(StandardCharsets.UTF_8));

        long txId = store.beginTransaction();
        store.put("status", "in-progress".getBytes(StandardCharsets.UTF_8), txId);

        // Outside reader sees initial value
        Optional<byte[]> readerVal = store.get("status");
        assertThat(readerVal).isPresent();
        assertThat(new String(readerVal.get(), StandardCharsets.UTF_8)).isEqualTo("initial");

        // Abort tx
        store.abort(txId);

        // Still initial
        assertThat(new String(store.get("status").get(), StandardCharsets.UTF_8)).isEqualTo("initial");
    }

    @Test
    @DisplayName("Read-Your-Own-Writes: active transaction sees its own uncommitted modifications")
    void readYourOwnWrites() {
        store.put("key1", "val1".getBytes(StandardCharsets.UTF_8));

        long txId = store.beginTransaction();
        store.put("key1", "modified-by-tx".getBytes(StandardCharsets.UTF_8), txId);

        // Transaction's own snapshot
        Snapshot txSnapshot = store.createSnapshotForTransaction(txId);
        Optional<byte[]> val = store.get("key1", txSnapshot);

        assertThat(val).isPresent();
        assertThat(new String(val.get(), StandardCharsets.UTF_8)).isEqualTo("modified-by-tx");

        txSnapshot.close();
        store.commit(txId);

        // After commit, all readers see it
        assertThat(new String(store.get("key1").get(), StandardCharsets.UTF_8)).isEqualTo("modified-by-tx");
    }

    @Test
    @DisplayName("Write-write conflict throws WriteConflictException")
    void writeWriteConflictDetected() {
        long tx1 = store.beginTransaction();
        long tx2 = store.beginTransaction();

        store.put("shared-resource", "tx1-data".getBytes(StandardCharsets.UTF_8), tx1);

        assertThatThrownBy(() -> store.put("shared-resource", "tx2-data".getBytes(StandardCharsets.UTF_8), tx2))
                .isInstanceOf(WriteConflictException.class)
                .hasMessageContaining("shared-resource");

        store.commit(tx1);
        store.abort(tx2);
    }

    @Test
    @DisplayName("Tombstone delete preserves historical visibility before deletion")
    void tombstoneDeletePreservesHistory() {
        store.put("temp-key", "alive".getBytes(StandardCharsets.UTF_8));

        Snapshot beforeDelete = store.createSnapshot();

        store.delete("temp-key");

        // Reader before delete sees "alive"
        assertThat(store.get("temp-key", beforeDelete)).isPresent();
        assertThat(new String(store.get("temp-key", beforeDelete).get(), StandardCharsets.UTF_8)).isEqualTo("alive");

        // Reader after delete sees empty
        assertThat(store.get("temp-key")).isEmpty();

        beforeDelete.close();
    }

    @Test
    @DisplayName("Snapshot scan returns all visible non-deleted keys")
    void snapshotScan() {
        store.put("k1", "v1".getBytes(StandardCharsets.UTF_8));
        store.put("k2", "v2".getBytes(StandardCharsets.UTF_8));
        store.put("k3", "v3".getBytes(StandardCharsets.UTF_8));
        store.delete("k2");

        Snapshot snapshot = store.createSnapshot();
        Map<String, byte[]> scan = store.scan(snapshot);

        assertThat(scan).containsOnlyKeys("k1", "k3");
        assertThat(new String(scan.get("k1"), StandardCharsets.UTF_8)).isEqualTo("v1");
        assertThat(new String(scan.get("k3"), StandardCharsets.UTF_8)).isEqualTo("v3");

        snapshot.close();
    }

    @Test
    @DisplayName("Raft snapshot serialization and deserialization restores exact visible state")
    void snapshotSerializationAndRestore() {
        store.put("k1", "v1".getBytes(StandardCharsets.UTF_8));
        store.put("k2", "v2".getBytes(StandardCharsets.UTF_8));
        store.put("k3", "v3".getBytes(StandardCharsets.UTF_8));
        store.delete("k2");

        byte[] serialized = store.serializeSnapshot();
        assertThat(serialized).isNotEmpty();

        MvccStore restoredStore = new MvccStore();
        restoredStore.restoreSnapshot(serialized);

        assertThat(restoredStore.get("k1")).isPresent();
        assertThat(new String(restoredStore.get("k1").get(), StandardCharsets.UTF_8)).isEqualTo("v1");
        assertThat(restoredStore.get("k2")).isEmpty();
        assertThat(restoredStore.get("k3")).isPresent();
        assertThat(new String(restoredStore.get("k3").get(), StandardCharsets.UTF_8)).isEqualTo("v3");
    }
}
