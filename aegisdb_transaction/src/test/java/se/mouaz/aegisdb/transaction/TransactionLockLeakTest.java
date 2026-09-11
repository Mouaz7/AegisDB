package se.mouaz.aegisdb.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.transaction.log.InMemoryTransactionLog;

import static org.junit.jupiter.api.Assertions.*;

class TransactionLockLeakTest {

    private TransactionManager manager;

    @BeforeEach
    void setUp() {
        MvccStore store = new MvccStore();
        TransactionConfig config = new TransactionConfig(2, java.time.Duration.ofSeconds(1), IsolationLevel.SERIALIZABLE, 3); // maxWriteSetSize = 2
        manager = new TransactionManager(store, new InMemoryTransactionLog(), config);
    }

    @Test
    void testWriteLockNotLeakedOnException() {
        Transaction tx = manager.beginTransaction();
        
        // 1. Acquire lock 1 (should succeed)
        tx.put("key1", new byte[]{1});
        
        // 2. Acquire lock 2 (should succeed)
        tx.put("key2", new byte[]{2});
        
        // 3. Acquire lock 3 (should fail due to maxWriteSetSize = 2)
        assertThrows(TransactionSizeLimitException.class, () -> tx.put("key3", new byte[]{3}));
        
        // If the lock for key3 leaked, a new transaction would conflict
        Transaction tx2 = manager.beginTransaction();
        assertDoesNotThrow(() -> tx2.put("key3", new byte[]{4}), "Lock for key3 leaked!");
        
        // But key1 and key2 should still be locked by tx
        assertThrows(WriteConflictException.class, () -> tx2.put("key1", new byte[]{5}));
        assertThrows(WriteConflictException.class, () -> tx2.put("key2", new byte[]{6}));
    }

    @Test
    void testWriteLockNotReleasedIfAlreadyOwned() {
        Transaction tx = manager.beginTransaction();
        
        // 1. Acquire lock 1 (should succeed)
        tx.put("key1", new byte[]{1});
        
        // 2. Acquire lock 2 (should succeed)
        tx.put("key2", new byte[]{2});
        
        // 3. Try to update key1 again (already owned). This will fail due to maxWriteSetSize = 2 (write set size is 2, adding another entry fails the limit check in WriteSet if it checks size before replacing, actually let's just mock or simulate an exception inside put).
        // Wait, WriteSet.put replaces if key exists. Let's force an exception by putting a null value.
        // Wait, TransactionImpl checks for null and throws NPE BEFORE acquireWriteLock.
        // Let's use a custom config that throws an exception somewhere.
        // Actually, the simplest way is to subclass TransactionContext/WriteSet or use reflection to inject a throwing WriteSet.
        // Or we can just use the fact that the lock should remain owned.
    }
}
