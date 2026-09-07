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
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("Three Node Election Test (Section 82)")
class ThreeNodeElectionTest {

    private RaftNode node1;
    private RaftNode node2;
    private RaftNode node3;

    private NodeId id1;
    private NodeId id2;
    private NodeId id3;

    private InMemoryTransport transport1;
    private InMemoryTransport transport2;
    private InMemoryTransport transport3;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        id1 = NodeId.of("raft-node-1");
        id2 = NodeId.of("raft-node-2");
        id3 = NodeId.of("raft-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 7001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 7002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 7003);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("three-node-cluster")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        transport1 = new InMemoryTransport(id1);
        transport2 = new InMemoryTransport(id2);
        transport3 = new InMemoryTransport(id3);

        transport1.start();
        transport2.start();
        transport3.start();

        // Node 1 has a short timeout (150ms), Node 2 & 3 have longer timeouts (500ms)
        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(120))
                .maxElectionTimeout(Duration.ofMillis(150))
                .heartbeatInterval(Duration.ofMillis(40))
                .random(new Random(42))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(500))
                .maxElectionTimeout(Duration.ofMillis(600))
                .heartbeatInterval(Duration.ofMillis(40))
                .random(new Random(43))
                .build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(500))
                .maxElectionTimeout(Duration.ofMillis(600))
                .heartbeatInterval(Duration.ofMillis(40))
                .random(new Random(44))
                .build();

        node1.start();
        node2.start();
        node3.start();
    }

    @AfterEach
    void tearDown() {
        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
        if (node3 != null) node3.stop();

        if (transport1 != null) transport1.stop();
        if (transport2 != null) transport2.stop();
        if (transport3 != null) transport3.stop();

        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("Acceptance Criterion: Exactly one leader is elected in a 3-node cluster")
    void testThreeNodeElectionElectsSingleLeader() {
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            long leaderCount = List.of(node1, node2, node3).stream()
                    .filter(n -> n.role() == RaftRole.LEADER)
                    .count();

            assertThat(leaderCount).isEqualTo(1L);
        });

        RaftNode leader = List.of(node1, node2, node3).stream()
                .filter(n -> n.role() == RaftRole.LEADER)
                .findFirst().orElseThrow();

        assertThat(leader.nodeId()).isEqualTo(id1);

        // Followers acknowledge the leader
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(node2.role()).isEqualTo(RaftRole.FOLLOWER);
            assertThat(node3.role()).isEqualTo(RaftRole.FOLLOWER);
            assertThat(node2.currentLeader()).contains(leader.nodeId());
            assertThat(node3.currentLeader()).contains(leader.nodeId());
        });
    }
}
