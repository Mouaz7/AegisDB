package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NetworkConfiguration;
import se.mouaz.aegisdb.common.NodeConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.NodeStatus;
import se.mouaz.aegisdb.node.DatabaseNode;
import se.mouaz.aegisdb.node.NodeBootstrap;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;
import se.mouaz.aegisdb.transport.TransportException;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Sprint 1 Acceptance Criteria - Real gRPC Cluster")
class GrpcThreeNodeClusterTest {

    private DatabaseNode node1;
    private DatabaseNode node2;
    private DatabaseNode node3;

    private NodeId id1;
    private NodeId id2;
    private NodeId id3;

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Could not find free port", e);
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        id1 = NodeId.of("grpc-node-1");
        id2 = NodeId.of("grpc-node-2");
        id3 = NodeId.of("grpc-node-3");

        int port1 = findFreePort();
        int port2 = findFreePort();
        int port3 = findFreePort();

        Endpoint ep1 = Endpoint.of("127.0.0.1", port1);
        Endpoint ep2 = Endpoint.of("127.0.0.1", port2);
        Endpoint ep3 = Endpoint.of("127.0.0.1", port3);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        NetworkConfiguration netConfig = new NetworkConfiguration(Duration.ofSeconds(1), Duration.ofSeconds(5), false, null, null, null);

        NodeConfiguration config1 = NodeConfiguration.builder()
                .nodeId(id1).endpoint(ep1).networkConfig(netConfig).build();
        NodeConfiguration config2 = NodeConfiguration.builder()
                .nodeId(id2).endpoint(ep2).networkConfig(netConfig).build();
        NodeConfiguration config3 = NodeConfiguration.builder()
                .nodeId(id3).endpoint(ep3).networkConfig(netConfig).build();

        node1 = NodeBootstrap.createGrpcNode(config1, clusterConfig);
        node2 = NodeBootstrap.createGrpcNode(config2, clusterConfig);
        node3 = NodeBootstrap.createGrpcNode(config3, clusterConfig);

        node1.start();
        node2.start();
        node3.start();
    }

    @AfterEach
    void tearDown() {
        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
        if (node3 != null) node3.stop();
    }

    @Test
    @DisplayName("Acceptance Criterion 1: Three gRPC nodes start successfully")
    void testThreeGrpcNodesStart() {
        assertThat(node1.status()).isEqualTo(NodeStatus.RUNNING);
        assertThat(node2.status()).isEqualTo(NodeStatus.RUNNING);
        assertThat(node3.status()).isEqualTo(NodeStatus.RUNNING);
    }

    @Test
    @DisplayName("Acceptance Criterion 2: gRPC nodes have unique identities and ports")
    void testUniqueIdentities() {
        Set<NodeId> uniqueIds = Set.of(node1.nodeId(), node2.nodeId(), node3.nodeId());
        assertThat(uniqueIds).hasSize(3);

        Set<Integer> uniquePorts = Set.of(
                node1.config().endpoint().port(),
                node2.config().endpoint().port(),
                node3.config().endpoint().port()
        );
        assertThat(uniquePorts).hasSize(3);
    }

    @Test
    @DisplayName("Acceptance Criterion 3: Node A can call Node B and Node C over gRPC")
    void testGrpcNodeCommunication() throws Exception {
        // Node 1 calls Node 2 with RequestVote
        RequestVoteRequest voteReq = new RequestVoteRequest(node1.nodeId(), 1, 0, 0);
        CompletableFuture<RequestVoteResponse> voteFuture = node1.sendRequestVote(node2.nodeId(), voteReq);
        RequestVoteResponse voteResp = voteFuture.get();

        assertThat(voteResp.term()).isEqualTo(1);
        assertThat(voteResp.voteGranted()).isTrue();

        // Node 1 calls Node 3 with AppendEntries
        byte[] payload = "data-payload".getBytes(StandardCharsets.UTF_8);
        AppendEntriesRequest appendReq = new AppendEntriesRequest(1, node1.nodeId(), 0, 0, payload, 0);
        CompletableFuture<AppendEntriesResponse> appendFuture = node1.sendAppendEntries(node3.nodeId(), appendReq);
        AppendEntriesResponse appendResp = appendFuture.get();

        assertThat(appendResp.term()).isEqualTo(1);
        assertThat(appendResp.success()).isTrue();
    }

    @Test
    @DisplayName("Acceptance Criterion 4 & 5: Error propagation when calling stopped node")
    void testErrorPropagationWhenNodeStops() {
        node3.stop();
        assertThat(node3.status()).isEqualTo(NodeStatus.STOPPED);

        RequestVoteRequest voteReq = new RequestVoteRequest(node1.nodeId(), 1, 0, 0);
        CompletableFuture<RequestVoteResponse> future = node1.sendRequestVote(node3.nodeId(), voteReq);

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TransportException.class);
    }

    @Test
    @DisplayName("Acceptance Criterion 6: gRPC nodes stop gracefully")
    void testGracefulShutdown() {
        for (DatabaseNode node : List.of(node1, node2, node3)) {
            assertThat(node.status()).isEqualTo(NodeStatus.RUNNING);
            node.stop();
            assertThat(node.status()).isEqualTo(NodeStatus.STOPPED);
        }
    }
}
