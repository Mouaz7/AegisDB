package se.mouaz.aegisdb.raft.replication;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CommitIndexManagerTest {

    private final CommitIndexManager manager = new CommitIndexManager();

    @Test
    @DisplayName("Advances commitIndex when majority replicates entries from current term")
    void advancesCommitIndexOnMajority() {
        RaftLog log = new RaftLog();
        log.append(new RaftLogEntry(1, 1, "d1".getBytes(StandardCharsets.UTF_8)));
        log.append(new RaftLogEntry(2, 1, "d2".getBytes(StandardCharsets.UTF_8)));
        log.append(new RaftLogEntry(3, 1, "d3".getBytes(StandardCharsets.UTF_8)));

        FollowerReplicationState f2 = new FollowerReplicationState(NodeId.of("node-2"), 4);
        f2.setMatchIndex(3);

        FollowerReplicationState f3 = new FollowerReplicationState(NodeId.of("node-3"), 4);
        f3.setMatchIndex(1);

        // 3-node cluster: leader (index 3) + node-2 (index 3) = 2 nodes >= majority (2)
        long newCommit = manager.computeNewCommitIndex(log, 1, 0, List.of(f2, f3), 3);
        assertThat(newCommit).isEqualTo(3);
    }

    @Test
    @DisplayName("Does not advance commitIndex if majority is not reached")
    void doesNotAdvanceWithoutMajority() {
        RaftLog log = new RaftLog();
        log.append(new RaftLogEntry(1, 1, "d1".getBytes(StandardCharsets.UTF_8)));
        log.append(new RaftLogEntry(2, 1, "d2".getBytes(StandardCharsets.UTF_8)));

        FollowerReplicationState f2 = new FollowerReplicationState(NodeId.of("node-2"), 3);
        f2.setMatchIndex(0);

        FollowerReplicationState f3 = new FollowerReplicationState(NodeId.of("node-3"), 3);
        f3.setMatchIndex(0);

        // Leader alone is 1/3 (minority)
        long newCommit = manager.computeNewCommitIndex(log, 1, 0, List.of(f2, f3), 3);
        assertThat(newCommit).isZero();
    }

    @Test
    @DisplayName("Ongaro §5.4.2: Never commits entries from previous terms by counting replicas directly")
    void neverCommitsPreviousTermDirectly() {
        RaftLog log = new RaftLog();
        // Entries 1 and 2 from term 1
        log.append(new RaftLogEntry(1, 1, "d1".getBytes(StandardCharsets.UTF_8)));
        log.append(new RaftLogEntry(2, 1, "d2".getBytes(StandardCharsets.UTF_8)));
        // Entry 3 from term 2
        log.append(new RaftLogEntry(3, 2, "d3".getBytes(StandardCharsets.UTF_8)));

        FollowerReplicationState f2 = new FollowerReplicationState(NodeId.of("node-2"), 4);
        f2.setMatchIndex(2); // Has replicated up to index 2

        FollowerReplicationState f3 = new FollowerReplicationState(NodeId.of("node-3"), 4);
        f3.setMatchIndex(2); // Also replicated up to index 2

        // Current term is 2! Even though index 2 has all 3 nodes, its term is 1 != 2
        // It cannot be directly committed
        long newCommit = manager.computeNewCommitIndex(log, 2, 0, List.of(f2, f3), 3);
        assertThat(newCommit).isZero();

        // But once entry 3 from current term 2 reaches majority:
        f2.setMatchIndex(3);
        long afterCurrentTermMajority = manager.computeNewCommitIndex(log, 2, 0, List.of(f2, f3), 3);
        assertThat(afterCurrentTermMajority).isEqualTo(3);
    }
}
