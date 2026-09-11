package se.mouaz.aegisdb.raft.event;

import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;

public record AppendEntriesResponseEvent(
        NodeId fromNode,
        long term,
        AppendEntriesResponse response
) implements RaftEvent {
}
