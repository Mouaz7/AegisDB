package se.mouaz.aegisdb.node;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.NodeStatus;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class DatabaseNodeTest {

    private DatabaseNode node1;
    private DatabaseNode node2;

    @BeforeEach
    void setUp() throws Exception {
        InMemoryTransport.clearRegistry();

        NodeId id1 = NodeId.of("test-node-1");
        NodeId id2 = NodeId.of("test-node-2");

        Endpoint ep1 = Endpoint.of("localhost", 7001);
        Endpoint ep2 = Endpoint.of("localhost", 7002);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .build();

        NodeConfiguration config1 = NodeConfiguration.builder()
                .nodeId(id1)
                .endpoint(ep1)
                .build();

        NodeConfiguration config2 = NodeConfiguration.builder()
                .nodeId(id2)
                .endpoint(ep2)
                .build();

        node1 = NodeBootstrap.createInMemoryNode(config1, clusterConfig);
        node2 = NodeBootstrap.createInMemoryNode(config2, clusterConfig);

        assertThat(node1.status()).isEqualTo(NodeStatus.STOPPED);
        assertThat(node2.status()).isEqualTo(NodeStatus.STOPPED);

        node1.start();
        node2.start();

        assertThat(node1.status()).isEqualTo(NodeStatus.RUNNING);
        assertThat(node2.status()).isEqualTo(NodeStatus.RUNNING);
    }

    @AfterEach
    void tearDown() {
        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
        InMemoryTransport.clearRegistry();
    }

    @Test
    void testNodeCommunication() throws Exception {
        RequestVoteRequest voteReq = new RequestVoteRequest(node1.nodeId(), 1, 0, 0);
        CompletableFuture<RequestVoteResponse> future = node1.sendRequestVote(node2.nodeId(), voteReq);

        RequestVoteResponse response = future.get();
        assertThat(response.term()).isEqualTo(1);
        assertThat(response.voteGranted()).isTrue();
    }

    @Test
    void testGracefulStop() {
        node1.stop();
        assertThat(node1.status()).isEqualTo(NodeStatus.STOPPED);

        node2.stop();
        assertThat(node2.status()).isEqualTo(NodeStatus.STOPPED);
    }
}
