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

    public NotLeaderException(NodeId currentLeader) {
        super(ErrorCode.NOT_LEADER, "Node is not the leader. Current leader: " + (currentLeader != null ? currentLeader : "unknown"));
        this.currentLeader = currentLeader;
    }

    public Optional<NodeId> currentLeader() {
        return Optional.ofNullable(currentLeader);
    }
}
