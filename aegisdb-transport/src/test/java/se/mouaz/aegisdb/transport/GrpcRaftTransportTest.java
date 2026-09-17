package se.mouaz.aegisdb.transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;

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

    private static int[] findFreePorts(int count) {
        ServerSocket[] sockets = new ServerSocket[count];
        int[] ports = new int[count];
        try {
            for (int i = 0; i < count; i++) {
                sockets[i] = new ServerSocket(0);
                ports[i] = sockets[i].getLocalPort();
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot find free ports", e);
        } finally {
            for (ServerSocket socket : sockets) {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException ignored) {}
                }
            }
        }
        return ports;
    }

    @BeforeEach
    void setUp() throws Exception {
        nodeA = NodeId.of("grpc-node-a");
        nodeB = NodeId.of("grpc-node-b");

        Exception lastException = null;
        for (int attempt = 1; attempt <= 5; attempt++) {
            tearDown();
            int[] ports = findFreePorts(2);

            Endpoint epA = Endpoint.of("127.0.0.1", ports[0]);
            Endpoint epB = Endpoint.of("127.0.0.1", ports[1]);

            ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                    .addMember(nodeA, epA)
                    .addMember(nodeB, epB)
                    .build();

            transportA = new GrpcRaftTransport(nodeA, epA, clusterConfig, Duration.ofSeconds(5));
            transportB = new GrpcRaftTransport(nodeB, epB, clusterConfig, Duration.ofSeconds(5));

            transportA.registerHandler(new DummyHandler(nodeA));
            transportB.registerHandler(new DummyHandler(nodeB));

            try {
                transportA.start();
                transportB.start();
                return;
            } catch (Exception e) {
                lastException = e;
                tearDown();
                Thread.sleep(100L * attempt);
            }
        }
        if (lastException != null) {
            throw lastException;
        }
    }

    @AfterEach
    void tearDown() {
        if (transportA != null) {
            try { transportA.stop(); } catch (Exception ignored) {}
            transportA = null;
        }
        if (transportB != null) {
            try { transportB.stop(); } catch (Exception ignored) {}
            transportB = null;
        }
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
