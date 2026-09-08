package se.mouaz.aegisdb.raft;

import se.mouaz.aegisdb.common.DatabaseException;
import se.mouaz.aegisdb.common.ErrorCode;
import se.mouaz.aegisdb.common.NodeId;

import java.util.Optional;

/**
 * Thrown when a write/replication proposal is attempted on a non-leader node (Section 83).
 */
public class NotLeaderException extends DatabaseException {
    private final NodeId currentLeader;
    private final long currentTerm;

    public NotLeaderException(NodeId currentLeader, long currentTerm) {
        super(ErrorCode.NOT_LEADER, "Node is not the leader. Current leader: " + (currentLeader != null ? currentLeader : "unknown") + ", term: " + currentTerm);
        this.currentLeader = currentLeader;
        this.currentTerm = currentTerm;
    }

    public NotLeaderException(NodeId currentLeader) {
        this(currentLeader, 0L);
    }

    public Optional<NodeId> currentLeader() {
        return Optional.ofNullable(currentLeader);
    }

    public long currentTerm() {
        return currentTerm;
    }
}
