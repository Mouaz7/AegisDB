package se.mouaz.aegisdb.transport;

import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

public interface RaftRequestHandler {
    CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest request);
    CompletableFuture<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest request);
}
