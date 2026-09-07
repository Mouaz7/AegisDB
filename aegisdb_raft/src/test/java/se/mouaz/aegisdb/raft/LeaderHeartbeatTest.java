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
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("Leader Heartbeat Test (Section 82)")
class LeaderHeartbeatTest {

    private RaftNode nodeA;
    private RaftNode nodeB;
    private NodeId idA;
    private NodeId idB;
    private InMemoryTransport transportA;
    private InMemoryTransport transportB;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        idA = NodeId.of("leader-a");
        idB = NodeId.of("follower-b");

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("heartbeat-cluster")
                .addMember(idA, Endpoint.of("127.0.0.1", 7001))
                .addMember(idB, Endpoint.of("127.0.0.1", 7002))
                .build();

        transportA = new InMemoryTransport(idA);
        transportB = new InMemoryTransport(idB);
        transportA.start();
        transportB.start();

        nodeA = RaftNode.builder()
                .nodeId(idA).clusterConfig(clusterConfig).transport(transportA)
                .minElectionTimeout(Duration.ofMillis(100))
                .maxElectionTimeout(Duration.ofMillis(130))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(10))
                .build();

        nodeB = RaftNode.builder()
                .nodeId(idB).clusterConfig(clusterConfig).transport(transportB)
                .minElectionTimeout(Duration.ofMillis(300))
                .maxElectionTimeout(Duration.ofMillis(400))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(20))
                .build();

        nodeA.start();
        nodeB.start();
    }

    @AfterEach
    void tearDown() {
        if (nodeA != null) nodeA.stop();
        if (nodeB != null) nodeB.stop();
        if (transportA != null) transportA.stop();
        if (transportB != null) transportB.stop();
        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("Follower resets timeout upon valid heartbeat and does not start an election")
    void testFollowerResetsTimeoutOnHeartbeat() throws InterruptedException {
        // Wait until Node A becomes leader
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(nodeA.role()).isEqualTo(RaftRole.LEADER);
            assertThat(nodeB.role()).isEqualTo(RaftRole.FOLLOWER);
            assertThat(nodeB.currentLeader()).contains(idA);
        });

        long initialTerm = nodeB.currentTerm();

        // Let heartbeats flow for 500 ms (exceeding Node B's election timeout of 300-400 ms)
        Thread.sleep(500);

        // Node B must still be FOLLOWER, term must not have increased, leader remains Node A
        assertThat(nodeB.role()).isEqualTo(RaftRole.FOLLOWER);
        assertThat(nodeB.currentTerm()).isEqualTo(initialTerm);
        assertThat(nodeB.currentLeader()).contains(idA);
    }
}
