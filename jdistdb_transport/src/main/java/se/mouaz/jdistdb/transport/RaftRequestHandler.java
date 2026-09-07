package se.mouaz.jdistdb.transport;

import se.mouaz.jdistdb.protocol.AppendEntriesRequest;
import se.mouaz.jdistdb.protocol.AppendEntriesResponse;
import se.mouaz.jdistdb.protocol.RequestVoteRequest;
import se.mouaz.jdistdb.protocol.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

public interface RaftRequestHandler {
    CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest request);
    CompletableFuture<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest request);
}
