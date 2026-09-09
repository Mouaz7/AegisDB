package se.mouaz.aegisdb.transaction.distributed;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DurableCoordinatorLogTest {

    @TempDir
    Path tempDir;

    private Path logPath;

    @BeforeEach
    void setUp() {
        logPath = tempDir.resolve("2pc-coordinator.wal");
    }

    @Test
    void testLogStateAndRecovery() throws IOException {
        TransactionId tx1 = TransactionId.of(101);
        TransactionId tx2 = TransactionId.of(102);

        Set<ShardId> shards1 = Set.of(ShardId.of("shard-0"), ShardId.of("shard-1"));
        Set<ShardId> shards2 = Set.of(ShardId.of("shard-1"), ShardId.of("shard-2"));

        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logPath)) {
            log.logState(tx1, TwoPhaseCommitState.PREPARING, shards1);
            log.logState(tx2, TwoPhaseCommitState.PREPARING, shards2);

            log.logState(tx1, TwoPhaseCommitState.COMMIT_DECIDED, shards1);
            log.logState(tx1, TwoPhaseCommitState.COMMITTED, shards1);

            log.logState(tx2, TwoPhaseCommitState.ABORT_DECIDED, shards2);
        }

        // Reopen log and recover state transitions
        try (DurableCoordinatorLog recoveredLog = new DurableCoordinatorLog(logPath)) {
            Map<TransactionId, CoordinatorLogEntry> entries = recoveredLog.recover();

            assertThat(entries).hasSize(2);
            assertThat(entries.get(tx1).state()).isEqualTo(TwoPhaseCommitState.COMMITTED);
            assertThat(entries.get(tx1).participants()).containsExactlyInAnyOrder(ShardId.of("shard-0"), ShardId.of("shard-1"));

            assertThat(entries.get(tx2).state()).isEqualTo(TwoPhaseCommitState.ABORT_DECIDED);
            assertThat(entries.get(tx2).participants()).containsExactlyInAnyOrder(ShardId.of("shard-1"), ShardId.of("shard-2"));
        }
    }

    @Test
    void testTornWriteTruncationRecovery() throws IOException {
        TransactionId tx1 = TransactionId.of(201);
        Set<ShardId> shards = Set.of(ShardId.of("shard-0"));

        try (DurableCoordinatorLog log = new DurableCoordinatorLog(logPath)) {
            log.logState(tx1, TwoPhaseCommitState.PREPARING, shards);
            log.logState(tx1, TwoPhaseCommitState.COMMIT_DECIDED, shards);
        }

        long validLength = Files.size(logPath);

        // Simulate a torn write by appending corrupted incomplete bytes at the end of the journal
        try (FileChannel fc = FileChannel.open(logPath, StandardOpenOption.WRITE)) {
            fc.position(validLength);
            ByteBuffer garbage = ByteBuffer.wrap(new byte[]{0x42, 0x13, 0x37, 0x00, 0x01, 0x02});
            fc.write(garbage);
        }

        // Recovery should detect bad checksum/truncated framing, truncate torn write, and recover valid entries
        try (DurableCoordinatorLog recoveredLog = new DurableCoordinatorLog(logPath)) {
            Map<TransactionId, CoordinatorLogEntry> entries = recoveredLog.recover();

            assertThat(entries).hasSize(1);
            assertThat(entries.get(tx1).state()).isEqualTo(TwoPhaseCommitState.COMMIT_DECIDED);
        }

        // Verify the file was truncated back to valid length
        assertThat(Files.size(logPath)).isEqualTo(validLength);
    }

    @Test
    void testInvalidMagicHeaderRejected() throws IOException {
        // Write invalid magic header
        try (FileChannel fc = FileChannel.open(logPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putInt(0xDEADBEEF); // Bad magic
            buf.put((byte) 1);
            buf.flip();
            fc.write(buf);
        }

        assertThatThrownBy(() -> new DurableCoordinatorLog(logPath))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Invalid magic header");
    }
}
