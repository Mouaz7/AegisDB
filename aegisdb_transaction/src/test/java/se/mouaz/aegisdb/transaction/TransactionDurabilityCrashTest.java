package se.mouaz.aegisdb.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.transaction.log.InMemoryTransactionLog;
import se.mouaz.aegisdb.transaction.log.TransactionLogEntry;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class TransactionDurabilityCrashTest {

    private MvccStore store;
    private InMemoryTransactionLog transactionLog;
    private TransactionManager manager;

    @BeforeEach
    void setUp() {
        store = new MvccStore();
        transactionLog = new InMemoryTransactionLog();
        manager = new TransactionManager(store, transactionLog, TransactionConfig.defaultConfig());
    }

    @Test
    void testCaseA_CrashBeforeCommitDecided() {
        Transaction tx = manager.beginTransaction();
        tx.put("key", new byte[]{1});
        
        // Simulate crash right after put (redo info is NOT durable yet since we log at prepare/commit)
        // Actually, let's simulate a crash BEFORE the COMMIT_DECIDED is durably logged.
        // Wait, if it crashes before logCommit() is called, the log has nothing but BEGIN.
        
        // Let's create a new manager to simulate restart and recovery
        TransactionManager recoveredManager = new TransactionManager(new MvccStore(), transactionLog, TransactionConfig.defaultConfig());
        recoveredManager.recoverFromLog();
        
        assertFalse(recoveredManager.registry().isCommitted(tx.id()), "Transaction must NOT recover as committed");
    }

    @Test
    void testCaseB_CrashAfterCommitDecidedButBeforeMvccVisible() {
        Transaction tx = manager.beginTransaction();
        tx.put("key", new byte[]{1});
        
        // We need to simulate the state where COMMIT_DECIDED is durably logged, but MVCC is not yet committed.
        // Since TransactionManager does this sequentially in commit(), we can just inject a log entry directly 
        // to simulate crashing right after transactionLog.logCommit() and before mvccStore.commit().
        
        // Get the transaction context to extract the write set
        TransactionImpl txImpl = (TransactionImpl) tx;
        
        // Inject COMMIT log entry (this acts as COMMIT_DECIDED in our WAL)
        transactionLog.logCommit(tx.id(), System.currentTimeMillis(), txImpl.context().writeSet());
        
        // Now simulate a crash by instantiating a new MvccStore and recovering the manager.
        MvccStore recoveredStore = new MvccStore();
        TransactionManager recoveredManager = new TransactionManager(recoveredStore, transactionLog, TransactionConfig.defaultConfig());
        recoveredManager.recoverFromLog();
        
        // Transaction should be recovered as COMMITTED because the irreversible decision was durable
        assertTrue(recoveredManager.registry().isCommitted(tx.id()), "Transaction MUST recover as committed");
        
        // Since MVCC was not updated before the crash, recovery MUST apply the writes to MVCC to finish the commit
        // Verify that the write was actually applied to the new MVCC store during recovery
        Optional<byte[]> recoveredValue = recoveredStore.get("key");
        assertTrue(recoveredValue.isPresent(), "Value must be present in MvccStore after recovery");
        assertArrayEquals(new byte[]{1}, recoveredValue.get(), "Value must match what was put in the transaction");
    }
}
