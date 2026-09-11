package se.mouaz.aegisdb.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.storage.wal.FsyncPolicy;
import se.mouaz.aegisdb.storage.wal.StorageIndex;
import se.mouaz.aegisdb.storage.wal.WalConfig;
import se.mouaz.aegisdb.storage.wal.WalManager;
import se.mouaz.aegisdb.storage.wal.WalWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DurableRaftLogTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("DurableRaftLog durably appends and truncates entries")
    void testDurableLogAppendsAndTruncates() throws IOException {
        WalConfig config = WalConfig.of(tempDir);
        StorageIndex index = new StorageIndex();

        try (WalManager walManager = new WalManager(config)) {
            walManager.openWriter();
            try (WalWriter writer = new WalWriter(walManager)) {

            DurableRaftLog log = new DurableRaftLog(writer, index);

            // Append entries 1..4
            log.append(new RaftLogEntry(1L, 1L, "CMD-1".getBytes(StandardCharsets.UTF_8)));
            log.append(new RaftLogEntry(2L, 1L, "CMD-2".getBytes(StandardCharsets.UTF_8)));
            log.append(new RaftLogEntry(3L, 2L, "CMD-3".getBytes(StandardCharsets.UTF_8)));
            log.append(new RaftLogEntry(4L, 2L, "CMD-4".getBytes(StandardCharsets.UTF_8)));

            assertThat(log.lastLogIndex()).isEqualTo(4L);
            assertThat(log.lastLogTerm()).isEqualTo(2L);
            assertThat(index.size()).isEqualTo(4L);

            // Truncate from index 3 with commitIndex=2
            log.truncateFrom(3L, 2L);

            assertThat(log.lastLogIndex()).isEqualTo(2L);
            assertThat(log.lastLogTerm()).isEqualTo(1L);
            assertThat(index.contains(3L)).isFalse();
            assertThat(index.contains(4L)).isFalse();
            assertThat(index.contains(2L)).isTrue();
        }
        }
    }
}
