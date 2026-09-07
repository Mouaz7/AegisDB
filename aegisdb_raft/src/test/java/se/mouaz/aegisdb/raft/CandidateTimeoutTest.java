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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("Candidate Timeout Test (Section 82)")
class CandidateTimeoutTest {

    private RaftNode node1;
    private NodeId id1;
    private NodeId id2;
    private InMemoryTransport transport1;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        id1 = NodeId.of("cand-node-1");
        id2 = NodeId.of("silent-peer-2");

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("cand-timeout-cluster")
                .addMember(id1, Endpoint.of("127.0.0.1", 7001))
                .addMember(id2, Endpoint.of("127.0.0.1", 7002))
                .build();

        transport1 = new InMemoryTransport(id1);
        transport1.start();

        // Node 2 never responds (drops messages / offline)
        InMemoryTransport transport2 = new InMemoryTransport(id2);
        transport2.setDropMessages(true);
        transport2.start();

        node1 = RaftNode.builder()
                .nodeId(id1)
                .clusterConfig(clusterConfig)
                .transport(transport1)
                .minElectionTimeout(Duration.ofMillis(100))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(40))
                .build();

        node1.start();
    }

    @AfterEach
    void tearDown() {
        if (node1 != null) node1.stop();
        if (transport1 != null) transport1.stop();
        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("Candidate times out without majority and increments term on subsequent election attempt")
    void testCandidateTimeoutIncreasesTerm() {
        // Since peer 2 is silent, Node 1 needs 2/2 votes for majority and cannot achieve it.
        // It must repeat elections, advancing terms each time.
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            assertThat(node1.role()).isEqualTo(RaftRole.CANDIDATE);
            assertThat(node1.currentTerm()).isGreaterThanOrEqualTo(2L);
        });
    }
}
