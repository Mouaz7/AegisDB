package se.mouaz.aegisdb.raft.event;

import se.mouaz.aegisdb.protocol.InstallSnapshotRequest;
import se.mouaz.aegisdb.protocol.InstallSnapshotResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Inbound event for InstallSnapshot RPC dispatched on the Raft event loop.
 */
public record InstallSnapshotEvent(
        InstallSnapshotRequest request,
        CompletableFuture<InstallSnapshotResponse> future
) implements RaftEvent {}
