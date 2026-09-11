package se.mouaz.aegisdb.raft.log;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RaftLogCompactionTest {

    private RaftLog log;

    @BeforeEach
    void setUp() {
        log = new RaftLog();
        // Append 10 entries
        for (int i = 1; i <= 10; i++) {
            log.append(new RaftLogEntry(i, 1, ("entry_" + i).getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void testInitialState() {
        assertThat(log.lastLogIndex()).isEqualTo(10L);
        assertThat(log.lastLogTerm()).isEqualTo(1L);
        assertThat(log.snapshotIndex()).isEqualTo(0L);
        assertThat(log.snapshotTerm()).isEqualTo(0L);
    }

    @Test
    void testCompactionRetainsSubsequentEntries() {
        // Compact up to index 5 in term 1
        log.compactUpTo(5L, 1L);

        assertThat(log.snapshotIndex()).isEqualTo(5L);
        assertThat(log.snapshotTerm()).isEqualTo(1L);
        assertThat(log.lastLogIndex()).isEqualTo(10L);
        assertThat(log.lastLogTerm()).isEqualTo(1L);

        // Compacted indices should return -1 term and empty entry
        assertThat(log.getTerm(3L)).isEqualTo(-1L);
        assertThat(log.getEntry(3L)).isEmpty();

        // Snapshot index itself should return snapshotTerm
        assertThat(log.getTerm(5L)).isEqualTo(1L);
        assertThat(log.getEntry(5L)).isEmpty(); // snapshot sentinel is not an entry

        // Active indices 6 to 10 should be intact
        for (int i = 6; i <= 10; i++) {
            assertThat(log.getTerm(i)).isEqualTo(1L);
            assertThat(log.getEntry(i)).isPresent();
            assertThat(new String(log.getEntry(i).get().data(), StandardCharsets.UTF_8))
                    .isEqualTo("entry_" + i);
        }

        // Slicing from index 5 or 6 should give entries 6-10
        List<RaftLogEntry> from5 = log.getEntriesFrom(5L);
        assertThat(from5).hasSize(5);
        assertThat(from5.get(0).index()).isEqualTo(6L);

        List<RaftLogEntry> from7 = log.getEntriesFrom(7L);
        assertThat(from7).hasSize(4);
        assertThat(from7.get(0).index()).isEqualTo(7L);
    }

    @Test
    void testCompactionAllEntries() {
        // Compact up to 10
        log.compactUpTo(10L, 1L);

        assertThat(log.snapshotIndex()).isEqualTo(10L);
        assertThat(log.snapshotTerm()).isEqualTo(1L);
        assertThat(log.lastLogIndex()).isEqualTo(10L);
        assertThat(log.lastLogTerm()).isEqualTo(1L);
        assertThat(log.allEntries()).isEmpty();

        // Append next entry 11
        log.append(new RaftLogEntry(11L, 2L, "entry_11".getBytes(StandardCharsets.UTF_8)));
        assertThat(log.lastLogIndex()).isEqualTo(11L);
        assertThat(log.lastLogTerm()).isEqualTo(2L);
        assertThat(log.getEntry(11L)).isPresent();
    }

    @Test
    void testTruncateSafetyWithSnapshot() {
        log.compactUpTo(5L, 1L);

        // Truncating before or at snapshotIndex should be disallowed
        assertThatThrownBy(() -> log.truncateFrom(4L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already compacted");

        assertThatThrownBy(() -> log.truncateFrom(5L, 0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("already compacted");

        // Truncate at 8
        log.truncateFrom(8L, 0L);
        assertThat(log.lastLogIndex()).isEqualTo(7L);
    }
}
