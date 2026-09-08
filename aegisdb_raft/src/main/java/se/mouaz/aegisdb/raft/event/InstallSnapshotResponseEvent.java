package se.mouaz.aegisdb.raft.event;

import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.InstallSnapshotResponse;

/**
 * Event delivered to leader when a follower replies to InstallSnapshot RPC.
 */
public record InstallSnapshotResponseEvent(
        NodeId fromNode,
        long term,
        InstallSnapshotResponse response,
        long snapshotIndex
) implements RaftEvent {}
