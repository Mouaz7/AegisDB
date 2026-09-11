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
import se.mouaz.aegisdb.transport.InMemoryTransport;
import se.mouaz.aegisdb.transport.TransportException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Sprint 1 Acceptance Criteria - InMemory Cluster")
class InMemoryThreeNodeClusterTest {

    private DatabaseNode nodeA;
    private DatabaseNode nodeB;
    private DatabaseNode nodeC;

    private NodeId idA;
    private NodeId idB;
    private NodeId idC;

    @BeforeEach
    void setUp() throws Exception {
        InMemoryTransport.clearRegistry();

        idA = NodeId.of("node-1");
        idB = NodeId.of("node-2");
        idC = NodeId.of("node-3");

        Endpoint epA = Endpoint.of("localhost", 7001);
        Endpoint epB = Endpoint.of("localhost", 7002);
        Endpoint epC = Endpoint.of("localhost", 7003);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .addMember(idA, epA)
                .addMember(idB, epB)
                .addMember(idC, epC)
                .build();

        NetworkConfiguration netConfig = new NetworkConfiguration(Duration.ofMillis(300), Duration.ofMillis(300), false, null, null, null);

        NodeConfiguration configA = NodeConfiguration.builder()
                .nodeId(idA).endpoint(epA).networkConfig(netConfig)
                .build();
        NodeConfiguration configB = NodeConfiguration.builder()
                .nodeId(idB).endpoint(epB).networkConfig(netConfig)
                .build();
        NodeConfiguration configC = NodeConfiguration.builder()
                .nodeId(idC).endpoint(epC).networkConfig(netConfig)
                .build();

        nodeA = NodeBootstrap.createInMemoryNode(configA, clusterConfig);
        nodeB = NodeBootstrap.createInMemoryNode(configB, clusterConfig);
        nodeC = NodeBootstrap.createInMemoryNode(configC, clusterConfig);

        nodeA.start();
        nodeB.start();
        nodeC.start();
    }

    @AfterEach
    void tearDown() {
        if (nodeA != null) nodeA.stop();
        if (nodeB != null) nodeB.stop();
        if (nodeC != null) nodeC.stop();
        InMemoryTransport.clearRegistry();
    }

    @Test
    @DisplayName("Acceptance Criterion 1: Three nodes start successfully")
    void testThreeNodesStart() {
        assertThat(nodeA.status()).isEqualTo(NodeStatus.RUNNING);
        assertThat(nodeB.status()).isEqualTo(NodeStatus.RUNNING);
        assertThat(nodeC.status()).isEqualTo(NodeStatus.RUNNING);
    }

    @Test
    @DisplayName("Acceptance Criterion 2: Nodes have unique identities")
    void testNodesHaveUniqueIdentities() {
        Set<NodeId> uniqueIds = Set.of(nodeA.nodeId(), nodeB.nodeId(), nodeC.nodeId());
        assertThat(uniqueIds).hasSize(3);

        Set<Endpoint> uniqueEndpoints = Set.of(
                nodeA.config().endpoint(),
                nodeB.config().endpoint(),
                nodeC.config().endpoint()
        );
        assertThat(uniqueEndpoints).hasSize(3);
    }

    @Test
    @DisplayName("Acceptance Criterion 3: Node A can call Node B and Node C")
    void testNodeCommunication() throws Exception {
        // Node A calls Node B with RequestVote
        RequestVoteRequest voteReq = new RequestVoteRequest(nodeA.nodeId(), 1, 0, 0);
        CompletableFuture<RequestVoteResponse> voteFuture = nodeA.sendRequestVote(nodeB.nodeId(), voteReq);
        RequestVoteResponse voteResp = voteFuture.get();

        assertThat(voteResp.term()).isEqualTo(1);
        assertThat(voteResp.voteGranted()).isTrue();

        // Node A calls Node C with AppendEntries
        byte[] payload = "test-log-entry".getBytes(StandardCharsets.UTF_8);
        AppendEntriesRequest appendReq = new AppendEntriesRequest(1, nodeA.nodeId(), 0, 0, payload, 0);
        CompletableFuture<AppendEntriesResponse> appendFuture = nodeA.sendAppendEntries(nodeC.nodeId(), appendReq);
        AppendEntriesResponse appendResp = appendFuture.get();

        assertThat(appendResp.term()).isEqualTo(1);
        assertThat(appendResp.success()).isTrue();

        // Node B calls Node A
        AppendEntriesRequest heartbeatReq = AppendEntriesRequest.heartbeat(1, nodeB.nodeId(), 0, 0, 0);
        AppendEntriesResponse heartbeatResp = nodeB.sendAppendEntries(nodeA.nodeId(), heartbeatReq).get();
        assertThat(heartbeatResp.success()).isTrue();
    }

    @Test
    @DisplayName("Acceptance Criterion 4: Timeout works when target is unresponsive or delayed")
    void testTimeoutWorks() {
        InMemoryTransport transportC = (InMemoryTransport) nodeC.transport();
        transportC.setDropMessages(true); // Simulates network silence / unresponsive node

        RequestVoteRequest voteReq = new RequestVoteRequest(nodeA.nodeId(), 1, 0, 0);
        CompletableFuture<RequestVoteResponse> future = nodeA.sendRequestVote(nodeC.nodeId(), voteReq);

        assertThatThrownBy(future::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TransportException.class);
    }

    @Test
    @DisplayName("Acceptance Criterion 5: Errors propagate correctly when target does not exist or is stopped")
    void testErrorPropagation() {
        // Calling non-existent node
        NodeId unknownId = NodeId.of("non-existent-node");
        RequestVoteRequest voteReq = new RequestVoteRequest(nodeA.nodeId(), 1, 0, 0);
        CompletableFuture<RequestVoteResponse> futureToUnknown = nodeA.sendRequestVote(unknownId, voteReq);

        assertThatThrownBy(futureToUnknown::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TransportException.class);

        // Stop Node B and call it
        nodeB.stop();
        assertThat(nodeB.status()).isEqualTo(NodeStatus.STOPPED);

        CompletableFuture<RequestVoteResponse> futureToStopped = nodeA.sendRequestVote(nodeB.nodeId(), voteReq);
        assertThatThrownBy(futureToStopped::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TransportException.class);
    }

    @Test
    @DisplayName("Acceptance Criterion 6: Nodes stop gracefully")
    void testNodesStopGracefully() {
        for (DatabaseNode node : List.of(nodeA, nodeB, nodeC)) {
            assertThat(node.status()).isEqualTo(NodeStatus.RUNNING);
            node.stop();
            assertThat(node.status()).isEqualTo(NodeStatus.STOPPED);
        }
    }
}
