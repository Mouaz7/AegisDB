package se.mouaz.jdistdb.transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.jdistdb.common.ClusterConfiguration;
import se.mouaz.jdistdb.common.Endpoint;
import se.mouaz.jdistdb.common.NodeId;
import se.mouaz.jdistdb.protocol.AppendEntriesRequest;
import se.mouaz.jdistdb.protocol.AppendEntriesResponse;
import se.mouaz.jdistdb.protocol.RequestVoteRequest;
import se.mouaz.jdistdb.protocol.RequestVoteResponse;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GrpcRaftTransportTest {

    private GrpcRaftTransport transportA;
    private GrpcRaftTransport transportB;
    private NodeId nodeA;
    private NodeId nodeB;

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Cannot find free port", e);
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        nodeA = NodeId.of("grpc-node-a");
        nodeB = NodeId.of("grpc-node-b");

        int portA = findFreePort();
        int portB = findFreePort();

        Endpoint epA = Endpoint.of("127.0.0.1", portA);
        Endpoint epB = Endpoint.of("127.0.0.1", portB);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .addMember(nodeA, epA)
                .addMember(nodeB, epB)
                .build();

        transportA = new GrpcRaftTransport(nodeA, epA, clusterConfig, Duration.ofMillis(800));
        transportB = new GrpcRaftTransport(nodeB, epB, clusterConfig, Duration.ofMillis(800));

        transportA.registerHandler(new DummyHandler(nodeA));
        transportB.registerHandler(new DummyHandler(nodeB));

        transportA.start();
        transportB.start();
    }

    @AfterEach
    void tearDown() {
        if (transportA != null) transportA.stop();
        if (transportB != null) transportB.stop();
    }

    @Test
    void testGrpcRequestVoteAndAppendEntries() throws Exception {
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
    void testGrpcTimeoutWhenDestinationStalls() {
        transportB.registerHandler(new RaftRequestHandler() {
            @Override
            public CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest request) {
                return new CompletableFuture<>(); // never completes
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
