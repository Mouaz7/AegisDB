package se.mouaz.aegisdb.raft.log;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RaftLogTest {

    @Test
    @DisplayName("New RaftLog starts with index 0 sentinel and size 0")
    void initialLogState() {
        RaftLog log = new RaftLog();
        assertThat(log.isEmpty()).isTrue();
        assertThat(log.size()).isZero();
        assertThat(log.lastLogIndex()).isZero();
        assertThat(log.lastLogTerm()).isZero();
        assertThat(log.getTerm(0)).isZero();
        assertThat(log.getEntry(0)).isEmpty();
    }

    @Test
    @DisplayName("Appends entries in contiguous 1-based order")
    void appendContiguousEntries() {
        RaftLog log = new RaftLog();

        RaftLogEntry e1 = new RaftLogEntry(1, 1, "cmd1".getBytes(StandardCharsets.UTF_8));
        RaftLogEntry e2 = new RaftLogEntry(2, 1, "cmd2".getBytes(StandardCharsets.UTF_8));
        RaftLogEntry e3 = new RaftLogEntry(3, 2, "cmd3".getBytes(StandardCharsets.UTF_8));

        log.append(e1);
        log.append(List.of(e2, e3));

        assertThat(log.size()).isEqualTo(3);
        assertThat(log.lastLogIndex()).isEqualTo(3);
        assertThat(log.lastLogTerm()).isEqualTo(2);

        assertThat(log.getTerm(1)).isEqualTo(1);
        assertThat(log.getTerm(2)).isEqualTo(1);
        assertThat(log.getTerm(3)).isEqualTo(2);
        assertThat(log.getTerm(4)).isEqualTo(-1);

        assertThat(log.getEntry(1)).contains(e1);
        assertThat(log.getEntry(2)).contains(e2);
        assertThat(log.getEntry(3)).contains(e3);
    }

    @Test
    @DisplayName("Reject non-contiguous entry append")
    void rejectNonContiguousAppend() {
        RaftLog log = new RaftLog();
        RaftLogEntry e2 = new RaftLogEntry(2, 1, "cmd2".getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> log.append(e2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Non-contiguous entry append");
    }

    @Test
    @DisplayName("Slicing entries with getEntriesFrom")
    void getEntriesFrom() {
        RaftLog log = new RaftLog();
        for (int i = 1; i <= 5; i++) {
            log.append(new RaftLogEntry(i, 1, ("val" + i).getBytes(StandardCharsets.UTF_8)));
        }

        List<RaftLogEntry> from3 = log.getEntriesFrom(3);
        assertThat(from3).hasSize(3);
        assertThat(from3.get(0).index()).isEqualTo(3);
        assertThat(from3.get(2).index()).isEqualTo(5);

        List<RaftLogEntry> capped = log.getEntriesFrom(2, 2);
        assertThat(capped).hasSize(2);
        assertThat(capped.get(0).index()).isEqualTo(2);
        assertThat(capped.get(1).index()).isEqualTo(3);

        assertThat(log.getEntriesFrom(10)).isEmpty();
    }

    @Test
    @DisplayName("Truncates uncommitted entries from given index upwards")
    void truncateFromUncommitted() {
        RaftLog log = new RaftLog();
        for (int i = 1; i <= 5; i++) {
            log.append(new RaftLogEntry(i, 1, ("val" + i).getBytes(StandardCharsets.UTF_8)));
        }

        log.truncateFrom(4, 2); // Truncate from 4, commitIndex is 2
        assertThat(log.lastLogIndex()).isEqualTo(3);
        assertThat(log.getEntry(4)).isEmpty();
        assertThat(log.getEntry(5)).isEmpty();
        assertThat(log.getEntry(3)).isPresent();
    }

    @Test
    @DisplayName("Throws exception if trying to truncate committed entries (Section 75 Invariant)")
    void cannotTruncateCommittedEntries() {
        RaftLog log = new RaftLog();
        for (int i = 1; i <= 5; i++) {
            log.append(new RaftLogEntry(i, 1, ("val" + i).getBytes(StandardCharsets.UTF_8)));
        }

        // commitIndex = 3, trying to truncate from 3 -> violation
        assertThatThrownBy(() -> log.truncateFrom(3, 3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot truncate committed entries");
    }

    @Test
    @DisplayName("Serializes and deserializes entry lists cleanly")
    void serializeRoundtrip() {
        List<RaftLogEntry> original = List.of(
                new RaftLogEntry(1, 1, "hello".getBytes(StandardCharsets.UTF_8)),
                new RaftLogEntry(2, 3, "world".getBytes(StandardCharsets.UTF_8))
        );

        byte[] bytes = RaftLogEntry.serializeList(original);
        List<RaftLogEntry> deserialized = RaftLogEntry.deserializeList(bytes);

        assertThat(deserialized).isEqualTo(original);
        assertThat(RaftLogEntry.deserializeList(new byte[0])).isEmpty();
        assertThat(RaftLogEntry.deserializeList(null)).isEmpty();
    }
}
