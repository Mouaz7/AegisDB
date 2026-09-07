package se.mouaz.aegisdb.raft.event;

import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;

import java.util.concurrent.CompletableFuture;

public record AppendEntriesEvent(
        AppendEntriesRequest request,
        CompletableFuture<AppendEntriesResponse> future
) implements RaftEvent {
}
