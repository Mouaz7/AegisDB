package se.mouaz.jdistdb.transport;

import se.mouaz.jdistdb.common.NodeId;
import se.mouaz.jdistdb.protocol.AppendEntriesRequest;
import se.mouaz.jdistdb.protocol.AppendEntriesResponse;
import se.mouaz.jdistdb.protocol.RequestVoteRequest;
import se.mouaz.jdistdb.protocol.RequestVoteResponse;

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
