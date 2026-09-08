package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.transport.GrpcRaftTransport;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("Sprint 2 Acceptance Criteria - Raft Election over Real gRPC")
class GrpcThreeNodeElectionTest {

    private RaftNode node1;
    private RaftNode node2;
    private RaftNode node3;

    private GrpcRaftTransport transport1;
    private GrpcRaftTransport transport2;
    private GrpcRaftTransport transport3;

    private static int findFreePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Could not find free port", e);
        }
    }

    @BeforeEach
    void setUp() throws Exception {
        RaftInvariants.clearInvariantTracking();

        NodeId id1 = NodeId.of("grpc-raft-1");
        NodeId id2 = NodeId.of("grpc-raft-2");
        NodeId id3 = NodeId.of("grpc-raft-3");

        int port1 = findFreePort();
        int port2 = findFreePort();
        int port3 = findFreePort();

        Endpoint ep1 = Endpoint.of("127.0.0.1", port1);
        Endpoint ep2 = Endpoint.of("127.0.0.1", port2);
        Endpoint ep3 = Endpoint.of("127.0.0.1", port3);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("grpc-raft-cluster")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        transport1 = new GrpcRaftTransport(id1, ep1, clusterConfig, Duration.ofSeconds(4));
        transport2 = new GrpcRaftTransport(id2, ep2, clusterConfig, Duration.ofSeconds(4));
        transport3 = new GrpcRaftTransport(id3, ep3, clusterConfig, Duration.ofSeconds(4));

        transport1.start();
        transport2.start();
        transport3.start();

        // Node 1 has fastest election timeout to win first
        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(250))
                .maxElectionTimeout(Duration.ofMillis(350))
                .heartbeatInterval(Duration.ofMillis(60))
                .random(new Random(1))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(600))
                .maxElectionTimeout(Duration.ofMillis(750))
                .heartbeatInterval(Duration.ofMillis(60))
                .random(new Random(2))
                .build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(800))
                .maxElectionTimeout(Duration.ofMillis(950))
                .heartbeatInterval(Duration.ofMillis(60))
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

        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("Acceptance Criteria 1, 2 & 3: Election, Heartbeat and Failover over live gRPC")
    void testGrpcRaftElectionAndFailover() {
        // 1. [AC1] Exactly one leader elected over gRPC
        await().atMost(Duration.ofSeconds(6)).untilAsserted(() -> {
            assertThat(node1.role()).isEqualTo(RaftRole.LEADER);
            assertThat(node2.role()).isEqualTo(RaftRole.FOLLOWER);
            assertThat(node3.role()).isEqualTo(RaftRole.FOLLOWER);
        });

        long initialTerm = node1.currentTerm();
        assertThat(initialTerm).isGreaterThanOrEqualTo(1L);

        // 2. [AC2] Followers recognize leader and receive gRPC heartbeats
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            assertThat(node2.currentLeader()).contains(node1.nodeId());
            assertThat(node3.currentLeader()).contains(node1.nodeId());
        });

        // 3. [AC3] Kill the gRPC leader, elect new leader with higher term
        node1.stop();
        transport1.stop();

        await().atMost(Duration.ofSeconds(6)).untilAsserted(() -> {
            List<RaftNode> alive = List.of(node2, node3);
            long leaderCount = alive.stream().filter(n -> n.role() == RaftRole.LEADER).count();
            assertThat(leaderCount).isEqualTo(1L);
        });

        RaftNode newLeader = (node2.role() == RaftRole.LEADER) ? node2 : node3;
        RaftNode newFollower = (newLeader == node2) ? node3 : node2;

        assertThat(newLeader.currentTerm()).isGreaterThan(initialTerm);
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            assertThat(newFollower.currentLeader()).contains(newLeader.nodeId());
        });
    }
}
