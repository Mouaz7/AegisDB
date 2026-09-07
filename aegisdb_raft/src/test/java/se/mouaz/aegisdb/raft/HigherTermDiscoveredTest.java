package se.mouaz.aegisdb.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("Higher Term Discovered Test (Section 82)")
class HigherTermDiscoveredTest {

    private RaftNode nodeA;
    private NodeId idA;
    private NodeId idB;
    private InMemoryTransport transportA;
    private InMemoryTransport transportB;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        idA = NodeId.of("node-a");
        idB = NodeId.of("node-b");

        transportA = new InMemoryTransport(idA);
        transportB = new InMemoryTransport(idB);
        transportA.start();
        transportB.start();
    }

    @AfterEach
    void tearDown() {
        if (nodeA != null) nodeA.stop();
        if (transportA != null) transportA.stop();
        if (transportB != null) transportB.stop();
        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("Leader steps down to FOLLOWER when receiving AppendEntries with higher term")
    void testLeaderStepsDownOnHigherTermAppendEntries() {
        // Single-node cluster so Node A readily becomes leader
        ClusterConfiguration singleNodeConfig = ClusterConfiguration.builder()
                .clusterId("higher-term-cluster")
                .addMember(idA, Endpoint.of("127.0.0.1", 7001))
                .build();

        nodeA = RaftNode.builder()
                .nodeId(idA)
                .clusterConfig(singleNodeConfig)
                .transport(transportA)
                .minElectionTimeout(Duration.ofMillis(80))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(30))
                .build();
        nodeA.start();

        // Wait until Node A becomes leader
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(nodeA.role()).isEqualTo(RaftRole.LEADER);
        });

        long termBefore = nodeA.currentTerm();
        long higherTerm = termBefore + 5;

        // Peer B sends periodic AppendEntries heartbeats with higherTerm to simulate live leader
        ScheduledExecutorService bHeartbeats = Executors.newSingleThreadScheduledExecutor();
        try {
            bHeartbeats.scheduleAtFixedRate(() -> {
                transportB.appendEntries(idA, AppendEntriesRequest.heartbeat(higherTerm, idB, 0, 0, 0));
            }, 0, 25, TimeUnit.MILLISECONDS);

            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                assertThat(nodeA.role()).isEqualTo(RaftRole.FOLLOWER);
                assertThat(nodeA.currentTerm()).isEqualTo(higherTerm);
                assertThat(nodeA.currentLeader()).contains(idB);
            });
        } finally {
            bHeartbeats.shutdownNow();
        }
    }

    @Test
    @DisplayName("Candidate steps down to FOLLOWER when receiving RequestVote with higher term")
    void testCandidateStepsDownOnHigherTermRequestVote() {
        ClusterConfiguration twoNodeConfig = ClusterConfiguration.builder()
                .clusterId("higher-term-cluster")
                .addMember(idA, Endpoint.of("127.0.0.1", 7001))
                .addMember(idB, Endpoint.of("127.0.0.1", 7002))
                .build();

        // Node A has enough election timeout window after stepping down
        nodeA = RaftNode.builder()
                .nodeId(idA)
                .clusterConfig(twoNodeConfig)
                .transport(transportA)
                .minElectionTimeout(Duration.ofMillis(500))
                .maxElectionTimeout(Duration.ofMillis(700))
                .heartbeatInterval(Duration.ofMillis(30))
                .build();
        nodeA.start();

        // Wait until Node A times out and becomes CANDIDATE for term >= 1
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(nodeA.role()).isEqualTo(RaftRole.CANDIDATE);
            assertThat(nodeA.currentTerm()).isGreaterThanOrEqualTo(1L);
        });

        long higherTerm = nodeA.currentTerm() + 10;
        RequestVoteRequest higherVoteReq = new RequestVoteRequest(idB, higherTerm, 0, 0);
        transportB.requestVote(idA, higherVoteReq);

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
            assertThat(nodeA.role()).isEqualTo(RaftRole.FOLLOWER);
            assertThat(nodeA.currentTerm()).isEqualTo(higherTerm);
        });
    }
}
