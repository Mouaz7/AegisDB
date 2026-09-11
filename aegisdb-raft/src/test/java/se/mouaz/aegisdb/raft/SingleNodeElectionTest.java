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
import se.mouaz.aegisdb.raft.time.DeterministicScheduler;
import se.mouaz.aegisdb.raft.time.TestClock;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Single Node Election Test (Section 82)")
class SingleNodeElectionTest {

    private RaftNode node;
    private NodeId nodeId;
    private InMemoryTransport transport;
    private DeterministicScheduler scheduler;
    private TestClock clock;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        nodeId = NodeId.of("single-node-1");
        Endpoint ep = Endpoint.of("localhost", 7001);
        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("test-single-cluster")
                .addMember(nodeId, ep)
                .build();

        transport = new InMemoryTransport(nodeId);
        transport.start();

        clock = new TestClock(1000L);
        scheduler = new DeterministicScheduler(clock);

        node = RaftNode.builder()
                .nodeId(nodeId)
                .clusterConfig(clusterConfig)
                .transport(transport)
                .clock(clock)
                .scheduler(scheduler)
                .minElectionTimeout(Duration.ofMillis(150))
                .maxElectionTimeout(Duration.ofMillis(150))
                .heartbeatInterval(Duration.ofMillis(50))
                .build();

        node.start();
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.stop();
        if (transport != null) transport.stop();
        if (scheduler != null) scheduler.close();
        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("Single node transitions to LEADER automatically when timeout expires")
    void testSingleNodeElectsSelf() {
        assertThat(node.role()).isEqualTo(RaftRole.FOLLOWER);

        // Advance virtual time past election timeout (150ms)
        scheduler.advanceTime(Duration.ofMillis(160));

        // In single node cluster, 1 of 1 vote constitutes a majority -> immediately LEADER
        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    assertThat(node.role()).isEqualTo(RaftRole.LEADER);
                    assertThat(node.currentTerm()).isEqualTo(1L);
                    assertThat(node.currentLeader()).contains(nodeId);
                });
    }
}
