package se.mouaz.aegisdb.transaction.log;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.transaction.TransactionConfig;
import se.mouaz.aegisdb.transaction.TransactionState;
import se.mouaz.aegisdb.transaction.WriteSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for DurableTransactionLog, CRC32 checksum framing, crash recovery, and torn-write handling
 * (Master Project Plan §8, §9 & §18).
 */
class DurableTransactionLogTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Durable log persists entries and replays cleanly across restarts")
    void persistAndReplayAcrossRestart() throws IOException {
        Path logFile = tempDir.resolve("tx-test.log");

        try (DurableTransactionLog log = new DurableTransactionLog(logFile)) {
            WriteSet ws = new WriteSet();
            ws.put("user:101", "Alice".getBytes(StandardCharsets.UTF_8), 100);

            log.logBegin(TransactionId.of(1), 100L);
            log.logPrepare(TransactionId.of(1), 101L, ws);
            log.logCommit(TransactionId.of(1), 105L, ws);
            log.logAbort(TransactionId.of(2), 106L);
        }

        // Reopen log from disk
        try (DurableTransactionLog recoveredLog = new DurableTransactionLog(logFile)) {
            List<TransactionLogEntry> entries = recoveredLog.replay();

            assertThat(entries).hasSize(4);
            assertThat(entries.get(0).state()).isEqualTo(TransactionState.ACTIVE);
            assertThat(entries.get(0).txId()).isEqualTo(TransactionId.of(1));

            assertThat(entries.get(1).state()).isEqualTo(TransactionState.PREPARING);
            assertThat(entries.get(1).writeOperations()).hasSize(1);
            assertThat(entries.get(1).writeOperations().get(0).key()).isEqualTo("user:101");

            assertThat(entries.get(2).state()).isEqualTo(TransactionState.COMMITTED);
            assertThat(entries.get(2).timestamp()).isEqualTo(105L);

            assertThat(entries.get(3).state()).isEqualTo(TransactionState.ABORTED);
            assertThat(entries.get(3).txId()).isEqualTo(TransactionId.of(2));
        }
    }

    @Test
    @DisplayName("Torn write at EOF is detected and truncated safely during recovery")
    void recoverFromTornWriteAtEof() throws IOException {
        Path logFile = tempDir.resolve("tx-torn.log");

        try (DurableTransactionLog log = new DurableTransactionLog(logFile)) {
            WriteSet ws = new WriteSet();
            ws.put("keyA", "valA".getBytes(StandardCharsets.UTF_8), 100);
            log.logBegin(TransactionId.of(10), 500L);
            log.logCommit(TransactionId.of(10), 505L, ws);
        }

        // Corrupt file by appending partial trailing junk bytes (simulating power failure mid-record)
        try (FileChannel fc = FileChannel.open(logFile, StandardOpenOption.WRITE)) {
            fc.position(fc.size());
            fc.write(ByteBuffer.wrap(new byte[]{0x01, 0x02, 0x03, 0x04})); // incomplete tail
        }

        // Reopen: recovery should detect torn tail, truncate to last valid record, and resume cleanly
        try (DurableTransactionLog recoveredLog = new DurableTransactionLog(logFile)) {
            List<TransactionLogEntry> entries = recoveredLog.replay();
            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).txId()).isEqualTo(TransactionId.of(10));
            assertThat(entries.get(1).state()).isEqualTo(TransactionState.COMMITTED);

            // Appending a new record after recovery succeeds
            recoveredLog.logAbort(TransactionId.of(11), 600L);
        }
    }
}
