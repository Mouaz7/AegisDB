package se.mouaz.jdistdb.transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.jdistdb.common.NodeId;
import se.mouaz.jdistdb.protocol.AppendEntriesRequest;
import se.mouaz.jdistdb.protocol.AppendEntriesResponse;
import se.mouaz.jdistdb.protocol.RequestVoteRequest;
import se.mouaz.jdistdb.protocol.RequestVoteResponse;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryTransportTest {

    private InMemoryTransport transportA;
    private InMemoryTransport transportB;
    private final NodeId nodeA = NodeId.of("node-a");
    private final NodeId nodeB = NodeId.of("node-b");

    @BeforeEach
    void setUp() {
        InMemoryTransport.clearRegistry();
        transportA = new InMemoryTransport(nodeA, Duration.ofMillis(300));
        transportB = new InMemoryTransport(nodeB, Duration.ofMillis(300));

        transportA.registerHandler(new DummyHandler(nodeA));
        transportB.registerHandler(new DummyHandler(nodeB));

        transportA.start();
        transportB.start();
    }

    @AfterEach
    void tearDown() {
        transportA.stop();
        transportB.stop();
        InMemoryTransport.clearRegistry();
    }

    @Test
    void testSuccessfulRequestVoteAndAppendEntries() throws Exception {
        RequestVoteRequest voteReq = new RequestVoteRequest(nodeA, 1, 0, 0);
        CompletableFuture<RequestVoteResponse> voteFuture = transportA.requestVote(nodeB, voteReq);
        RequestVoteResponse voteResp = voteFuture.get();

        assertThat(voteResp.term()).isEqualTo(1);
        assertThat(voteResp.voteGranted()).isTrue();

        AppendEntriesRequest appendReq = AppendEntriesRequest.heartbeat(1, nodeA, 0, 0, 0);
        CompletableFuture<AppendEntriesResponse> appendFuture = transportA.appendEntries(nodeB, appendReq);
        AppendEntriesResponse appendResp = appendFuture.get();

        assertThat(appendResp.term()).isEqualTo(1);
        assertThat(appendResp.success()).isTrue();
    }

    @Test
    void testTimeoutWhenTargetIsDelayed() {
        transportB.registerHandler(new RaftRequestHandler() {
            @Override
            public CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest request) {
                // Return a future that never completes
                return new CompletableFuture<>();
            }

            @Override
            public CompletableFuture<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest request) {
                return new CompletableFuture<>();
            }
        });

        RequestVoteRequest voteReq = new RequestVoteRequest(nodeA, 1, 0, 0);
        CompletableFuture<RequestVoteResponse> future = transportA.requestVote(nodeB, voteReq);

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TransportException.class)
                .hasMessageContaining("timed out");
    }

    @Test
    void testNetworkPartitionCausesTimeout() {
        InMemoryTransport.partition(Set.of(nodeA), Set.of(nodeB));

        RequestVoteRequest voteReq = new RequestVoteRequest(nodeA, 1, 0, 0);
        CompletableFuture<RequestVoteResponse> future = transportA.requestVote(nodeB, voteReq);

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TransportException.class);

        InMemoryTransport.healPartitions();
        CompletableFuture<RequestVoteResponse> healedFuture = transportA.requestVote(nodeB, voteReq);
        assertThat(healedFuture).isCompleted();
    }

    @Test
    void testCallToNonExistentNodeFails() {
        NodeId unknown = NodeId.of("unknown-node");
        RequestVoteRequest voteReq = new RequestVoteRequest(nodeA, 1, 0, 0);
        CompletableFuture<RequestVoteResponse> future = transportA.requestVote(unknown, voteReq);

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TransportException.class);
    }

    private static class DummyHandler implements RaftRequestHandler {
        private final NodeId nodeId;

        DummyHandler(NodeId nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest request) {
            return CompletableFuture.completedFuture(RequestVoteResponse.granted(request.term()));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest request) {
            return CompletableFuture.completedFuture(AppendEntriesResponse.success(request.term(), 1));
        }
    }
}
