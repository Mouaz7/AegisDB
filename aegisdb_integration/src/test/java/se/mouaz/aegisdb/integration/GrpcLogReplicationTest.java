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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("Phase 3 Acceptance Criteria - Raft Log Replication over Real gRPC (US006)")
class GrpcLogReplicationTest {

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

        NodeId id1 = NodeId.of("grpc-rep-1");
        NodeId id2 = NodeId.of("grpc-rep-2");
        NodeId id3 = NodeId.of("grpc-rep-3");

        int port1 = findFreePort();
        int port2 = findFreePort();
        int port3 = findFreePort();

        Endpoint ep1 = Endpoint.of("127.0.0.1", port1);
        Endpoint ep2 = Endpoint.of("127.0.0.1", port2);
        Endpoint ep3 = Endpoint.of("127.0.0.1", port3);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("grpc-log-cluster")
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

        // Node 1 is configured to become leader first
        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(200))
                .maxElectionTimeout(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofMillis(50))
                .random(new Random(101))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(600))
                .maxElectionTimeout(Duration.ofMillis(800))
                .heartbeatInterval(Duration.ofMillis(50))
                .random(new Random(102))
                .build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(700))
                .maxElectionTimeout(Duration.ofMillis(900))
                .heartbeatInterval(Duration.ofMillis(50))
                .random(new Random(103))
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
    }

    @Test
    @DisplayName("Leader proposes client writes, replicates to followers and commits over gRPC sockets")
    void writesReplicateAndCommitOverGrpc() throws Exception {
        // 1. Await leader election in the cluster
        await().atMost(Duration.ofSeconds(8)).until(() ->
                node1.role() == RaftRole.LEADER || node2.role() == RaftRole.LEADER || node3.role() == RaftRole.LEADER);

        RaftNode leader = node1.role() == RaftRole.LEADER ? node1 :
                (node2.role() == RaftRole.LEADER ? node2 : node3);

        List<RaftNode> followers = java.util.List.of(node1, node2, node3).stream()
                .filter(n -> n != leader)
                .toList();

        // 2. Propose 3 sequential writes through the leader
        for (int i = 1; i <= 3; i++) {
            byte[] cmd = ("grpc-command-" + i).getBytes(StandardCharsets.UTF_8);
            CompletableFuture<Long> future = leader.propose(cmd);
            Long committedIndex = future.get(5, TimeUnit.SECONDS);
            assertThat(committedIndex).isEqualTo((long) i);
        }

        assertThat(leader.commitIndex()).isEqualTo(3L);

        // 3. Await followers to receive all entries and advance their commitIndex over gRPC
        await().atMost(Duration.ofSeconds(8)).until(() ->
                followers.stream().allMatch(f -> f.commitIndex() == 3L));

        for (RaftNode f : followers) {
            assertThat(f.log().lastLogIndex()).isEqualTo(3L);
            for (int i = 1; i <= 3; i++) {
                assertThat(new String(f.log().getEntry(i).orElseThrow().data(), StandardCharsets.UTF_8))
                        .isEqualTo("grpc-command-" + i);
            }
        }

        // 5. Check Raft Invariants across all nodes
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node2.log(), 3L);
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), 3L);
    }
}
