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

@DisplayName("Split Vote Test (Section 82)")
class SplitVoteTest {

    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<InMemoryTransport> transports = new ArrayList<>();

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        ClusterConfiguration.Builder clusterBuilder = ClusterConfiguration.builder()
                .clusterId("split-vote-cluster");

        for (int i = 1; i <= 4; i++) {
            NodeId id = NodeId.of("split-node-" + i);
            Endpoint ep = Endpoint.of("127.0.0.1", 7010 + i);
            clusterBuilder.addMember(id, ep);
        }
        ClusterConfiguration clusterConfig = clusterBuilder.build();

        // Nodes 1 and 2 start with identical election timeouts to induce a split vote initially,
        // then randomize on the next round to resolve it.
        for (int i = 1; i <= 4; i++) {
            NodeId id = NodeId.of("split-node-" + i);
            InMemoryTransport transport = new InMemoryTransport(id);
            transport.start();
            transports.add(transport);

            RaftNode node = RaftNode.builder()
                    .nodeId(id)
                    .clusterConfig(clusterConfig)
                    .transport(transport)
                    .minElectionTimeout(Duration.ofMillis(120))
                    .maxElectionTimeout(Duration.ofMillis(250))
                    .heartbeatInterval(Duration.ofMillis(30))
                    .random(new Random(i * 31L))
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
    @DisplayName("Randomized timeouts resolve split votes and elect exactly one leader")
    void testSplitVoteResolvesToSingleLeader() {
        await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
            long leaderCount = nodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER)
                    .count();

            assertThat(leaderCount).isEqualTo(1L);
        });

        // Verify that all 3 remaining nodes became followers acknowledging the leader
        RaftNode leader = nodes.stream()
                .filter(n -> n.role() == RaftRole.LEADER)
                .findFirst().orElseThrow();

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            long followerCount = nodes.stream()
                    .filter(n -> n.role() == RaftRole.FOLLOWER && n.currentLeader().isPresent())
                    .count();
            assertThat(followerCount).isEqualTo(3L);
        });
    }
}
