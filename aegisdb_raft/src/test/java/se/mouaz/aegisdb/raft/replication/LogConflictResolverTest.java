package se.mouaz.aegisdb.raft.replication;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.raft.state.VolatileRaftState;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LogConflictResolverTest {

    private final LogConflictResolver resolver = new LogConflictResolver();
    private final NodeId leaderId = NodeId.of("leader-1");

    @Test
    @DisplayName("Rejects when prevLogIndex is higher than follower's lastLogIndex")
    void rejectMissingPrevLogIndex() {
        RaftLog log = new RaftLog();
        VolatileRaftState vState = new VolatileRaftState();

        AppendEntriesRequest req = new AppendEntriesRequest(
                1, leaderId, 5, 1, new byte[0], 0
        );

        AppendEntriesResponse resp = resolver.resolveAndAppend(log, req, vState, 1);
        assertThat(resp.success()).isFalse();
        assertThat(resp.matchIndex()).isZero();
    }

    @Test
    @DisplayName("Rejects when term at prevLogIndex does not match prevLogTerm")
    void rejectPrevLogTermMismatch() {
        RaftLog log = new RaftLog();
        log.append(new RaftLogEntry(1, 1, "d1".getBytes(StandardCharsets.UTF_8)));
        VolatileRaftState vState = new VolatileRaftState();

        // Leader says prevLogIndex=1, prevLogTerm=2, but follower has term 1 at index 1
        AppendEntriesRequest req = new AppendEntriesRequest(
                2, leaderId, 1, 2, new byte[0], 0
        );

        AppendEntriesResponse resp = resolver.resolveAndAppend(log, req, vState, 2);
        assertThat(resp.success()).isFalse();
    }

    @Test
    @DisplayName("Appends new entries when prevLog matches and updates commitIndex")
    void appendsEntriesAndUpdatesCommit() {
        RaftLog log = new RaftLog();
        VolatileRaftState vState = new VolatileRaftState();

        List<RaftLogEntry> entries = List.of(
                new RaftLogEntry(1, 1, "k1=v1".getBytes(StandardCharsets.UTF_8)),
                new RaftLogEntry(2, 1, "k2=v2".getBytes(StandardCharsets.UTF_8))
        );

        AppendEntriesRequest req = new AppendEntriesRequest(
                1, leaderId, 0, 0, RaftLogEntry.serializeList(entries), 2
        );

        AppendEntriesResponse resp = resolver.resolveAndAppend(log, req, vState, 1);
        assertThat(resp.success()).isTrue();
        assertThat(resp.matchIndex()).isEqualTo(2);
        assertThat(log.lastLogIndex()).isEqualTo(2);
        assertThat(vState.commitIndex()).isEqualTo(2);
    }

    @Test
    @DisplayName("Conflicting uncommitted entries are repaired (truncated and replaced)")
    void repairsConflictingEntries() {
        RaftLog log = new RaftLog();
        VolatileRaftState vState = new VolatileRaftState();

        // Follower has entries 1, 2 from term 1, and conflicting entry 3 from term 1
        log.append(new RaftLogEntry(1, 1, "e1".getBytes(StandardCharsets.UTF_8)));
        log.append(new RaftLogEntry(2, 1, "e2".getBytes(StandardCharsets.UTF_8)));
        log.append(new RaftLogEntry(3, 1, "stale_e3".getBytes(StandardCharsets.UTF_8)));

        // Leader in term 2 sends replacement for index 3 and new entry 4
        List<RaftLogEntry> leaderEntries = List.of(
                new RaftLogEntry(3, 2, "authoritative_e3".getBytes(StandardCharsets.UTF_8)),
                new RaftLogEntry(4, 2, "authoritative_e4".getBytes(StandardCharsets.UTF_8))
        );

        AppendEntriesRequest req = new AppendEntriesRequest(
                2, leaderId, 2, 1, RaftLogEntry.serializeList(leaderEntries), 3
        );

        AppendEntriesResponse resp = resolver.resolveAndAppend(log, req, vState, 2);
        assertThat(resp.success()).isTrue();
        assertThat(resp.matchIndex()).isEqualTo(4);

        assertThat(log.lastLogIndex()).isEqualTo(4);
        assertThat(log.getEntry(3).get().term()).isEqualTo(2);
        assertThat(new String(log.getEntry(3).get().data(), StandardCharsets.UTF_8)).isEqualTo("authoritative_e3");
        assertThat(log.getEntry(4).get().term()).isEqualTo(2);
        assertThat(vState.commitIndex()).isEqualTo(3);
    }
}
