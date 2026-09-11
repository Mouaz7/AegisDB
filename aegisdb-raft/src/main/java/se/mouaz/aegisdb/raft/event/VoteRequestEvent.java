package se.mouaz.aegisdb.raft.event;

import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

public record VoteRequestEvent(
        RequestVoteRequest request,
        CompletableFuture<RequestVoteResponse> future
) implements RaftEvent {
}
