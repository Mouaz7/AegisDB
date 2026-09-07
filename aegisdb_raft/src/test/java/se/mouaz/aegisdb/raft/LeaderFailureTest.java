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

@DisplayName("Leader Failure & Re-election Test (Section 82)")
class LeaderFailureTest {

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

        id1 = NodeId.of("failover-node-1");
        id2 = NodeId.of("failover-node-2");
        id3 = NodeId.of("failover-node-3");

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("failover-cluster")
                .addMember(id1, Endpoint.of("127.0.0.1", 7001))
                .addMember(id2, Endpoint.of("127.0.0.1", 7002))
                .addMember(id3, Endpoint.of("127.0.0.1", 7003))
                .build();

        transport1 = new InMemoryTransport(id1);
        transport2 = new InMemoryTransport(id2);
        transport3 = new InMemoryTransport(id3);

        transport1.start();
        transport2.start();
        transport3.start();

        // Node 1 becomes leader first
        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(100))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(1))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(300))
                .maxElectionTimeout(Duration.ofMillis(380))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(2))
                .build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(400))
                .maxElectionTimeout(Duration.ofMillis(480))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(3))
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
    @DisplayName("Acceptance Criterion: When leader fails, followers detect timeout and elect a new leader")
    void testNewLeaderElectedAfterLeaderFailure() {
        // 1. Wait for Node 1 to become initial leader
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(node1.role()).isEqualTo(RaftRole.LEADER);
            assertThat(node2.role()).isEqualTo(RaftRole.FOLLOWER);
            assertThat(node3.role()).isEqualTo(RaftRole.FOLLOWER);
        });

        long initialTerm = node1.currentTerm();

        // 2. Kill the leader (Node 1)
        node1.stop();
        transport1.stop();

        // 3. Verify that a new leader is elected from the remaining live nodes (Node 2 or 3)
        await().atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
            long liveLeaderCount = List.of(node2, node3).stream()
                    .filter(n -> n.role() == RaftRole.LEADER)
                    .count();

            assertThat(liveLeaderCount).isEqualTo(1L);
        });

        RaftNode newLeader = (node2.role() == RaftRole.LEADER) ? node2 : node3;
        RaftNode newFollower = (newLeader == node2) ? node3 : node2;

        assertThat(newLeader.currentTerm()).isGreaterThan(initialTerm);
        assertThat(newFollower.currentLeader()).contains(newLeader.nodeId());
    }
}
