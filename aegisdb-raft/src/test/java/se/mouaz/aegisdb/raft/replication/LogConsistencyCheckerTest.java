package se.mouaz.aegisdb.raft.replication;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class LogConsistencyCheckerTest {

    private final LogConsistencyChecker checker = new LogConsistencyChecker();
    private final NodeId leaderId = NodeId.of("leader-1");

    @Test
    @DisplayName("Consistency check succeeds when prevLogIndex is 0")
    void checkSucceedsAtSentinelZero() {
        RaftLog log = new RaftLog();
        AppendEntriesRequest req = new AppendEntriesRequest(1, leaderId, 0, 0, new byte[0], 0);

        LogConsistencyChecker.ConsistencyResult result = checker.check(log, req);
        assertThat(result.consistent()).isTrue();
    }

    @Test
    @DisplayName("Consistency check fails when follower log is shorter than prevLogIndex")
    void checkFailsWhenLogTooShort() {
        RaftLog log = new RaftLog();
        log.append(new RaftLogEntry(1, 1, "d1".getBytes(StandardCharsets.UTF_8)));

        AppendEntriesRequest req = new AppendEntriesRequest(1, leaderId, 4, 1, new byte[0], 0);
        LogConsistencyChecker.ConsistencyResult result = checker.check(log, req);

        assertThat(result.consistent()).isFalse();
        assertThat(result.conflictHintIndex()).isEqualTo(1L);
    }

    @Test
    @DisplayName("Consistency check fails when term at prevLogIndex does not match prevLogTerm")
    void checkFailsOnTermMismatch() {
        RaftLog log = new RaftLog();
        log.append(new RaftLogEntry(1, 1, "d1".getBytes(StandardCharsets.UTF_8)));

        // Leader expects term 2 at index 1, but follower has term 1
        AppendEntriesRequest req = new AppendEntriesRequest(2, leaderId, 1, 2, new byte[0], 0);
        LogConsistencyChecker.ConsistencyResult result = checker.check(log, req);

        assertThat(result.consistent()).isFalse();
        assertThat(result.conflictHintIndex()).isZero();
    }

    @Test
    @DisplayName("Consistency check succeeds when both prevLogIndex and prevLogTerm match")
    void checkSucceedsOnMatch() {
        RaftLog log = new RaftLog();
        log.append(new RaftLogEntry(1, 1, "d1".getBytes(StandardCharsets.UTF_8)));
        log.append(new RaftLogEntry(2, 2, "d2".getBytes(StandardCharsets.UTF_8)));

        AppendEntriesRequest req = new AppendEntriesRequest(2, leaderId, 2, 2, new byte[0], 2);
        LogConsistencyChecker.ConsistencyResult result = checker.check(log, req);

        assertThat(result.consistent()).isTrue();
        assertThat(result.conflictHintIndex()).isEqualTo(2L);
    }
}
