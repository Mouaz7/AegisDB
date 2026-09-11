package se.mouaz.aegisdb.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("Five Node Election Test (Section 82)")
class FiveNodeElectionTest {

    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<InMemoryTransport> transports = new ArrayList<>();

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        ClusterConfiguration.Builder clusterBuilder = ClusterConfiguration.builder()
                .clusterId("five-node-cluster");

        for (int i = 1; i <= 5; i++) {
            NodeId id = NodeId.of("node-" + i);
            Endpoint ep = Endpoint.of("127.0.0.1", 7000 + i);
            clusterBuilder.addMember(id, ep);
        }
        ClusterConfiguration clusterConfig = clusterBuilder.build();

        for (int i = 1; i <= 5; i++) {
            NodeId id = NodeId.of("node-" + i);
            InMemoryTransport transport = new InMemoryTransport(id);
            transport.start();
            transports.add(transport);

            // Node 1 has fastest timeout
            Duration minTimeout = (i == 1) ? Duration.ofMillis(120) : Duration.ofMillis(450 + i * 50);
            Duration maxTimeout = (i == 1) ? Duration.ofMillis(150) : Duration.ofMillis(600 + i * 50);

            RaftNode node = RaftNode.builder()
                    .nodeId(id)
                    .clusterConfig(clusterConfig)
                    .transport(transport)
                    .minElectionTimeout(minTimeout)
                    .maxElectionTimeout(maxTimeout)
                    .heartbeatInterval(Duration.ofMillis(40))
                    .random(new Random(100 + i))
                    .build();

            nodes.add(node);
        }

        nodes.forEach(RaftNode::start);
    }

    @AfterEach
    void tearDown() {
        nodes.forEach(RaftNode::stop);
        transports.forEach(InMemoryTransport::stop);
        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("5 nodes achieve quorum (> 5/2 = 3 votes) and elect exactly one leader")
    void testFiveNodesElectSingleLeader() {
        await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
            long leaderCount = nodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER)
                    .count();

            assertThat(leaderCount).isEqualTo(1L);
        });

        RaftNode leader = nodes.stream()
                .filter(n -> n.role() == RaftRole.LEADER)
                .findFirst().orElseThrow();

        assertThat(leader.nodeId()).isEqualTo(NodeId.of("node-1"));

        // All 4 other nodes become followers and recognize the leader
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            long followerCount = nodes.stream()
                    .filter(n -> n.role() == RaftRole.FOLLOWER && n.currentLeader().isPresent())
                    .count();
            assertThat(followerCount).isEqualTo(4L);
        });
    }
}
