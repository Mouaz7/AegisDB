package se.mouaz.aegisdb.transport;

import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface RaftTransport extends AutoCloseable {
    NodeId localNodeId();
    CompletableFuture<RequestVoteResponse> requestVote(NodeId destination, RequestVoteRequest request);
    CompletableFuture<AppendEntriesResponse> appendEntries(NodeId destination, AppendEntriesRequest request);
    void registerHandler(RaftRequestHandler handler);
    void start() throws IOException;
    void stop();

    @Override
    default void close() {
        stop();
    }
}
