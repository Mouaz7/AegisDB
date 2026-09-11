package se.mouaz.aegisdb.raft.event;

import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;

public record VoteResponseEvent(
        NodeId fromNode,
        long term,
        RequestVoteResponse response
) implements RaftEvent {
}
