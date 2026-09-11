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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("Log Replication Test - Acceptance Criteria 1 & 3 (US006)")
class LogReplicationTest {

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

        id1 = NodeId.of("rep-node-1");
        id2 = NodeId.of("rep-node-2");
        id3 = NodeId.of("rep-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 7101);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 7102);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 7103);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("rep-cluster")
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

        // Node 1 is biased to become leader quickly
        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(80))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(42))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(400))
                .maxElectionTimeout(Duration.ofMillis(500))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(43))
                .build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(400))
                .maxElectionTimeout(Duration.ofMillis(500))
                .heartbeatInterval(Duration.ofMillis(30))
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
    }

    @Test
    @DisplayName("Writes replicate to followers and commitIndex advances")
    void writesReplicateAndCommitIndexAdvances() throws Exception {
        // 1. Wait for leader election
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);
        await().atMost(Duration.ofSeconds(2)).until(() -> node2.role() == RaftRole.FOLLOWER && node3.role() == RaftRole.FOLLOWER);

        // 2. Client proposes a write command to the leader
        byte[] command1 = "SET user:1 'Alice'".getBytes(StandardCharsets.UTF_8);
        CompletableFuture<Long> future1 = node1.propose(command1);

        // 3. Verify write commits and returns index 1
        Long committedIndex1 = future1.get(3, TimeUnit.SECONDS);
        assertThat(committedIndex1).isEqualTo(1L);
        assertThat(node1.commitIndex()).isEqualTo(1L);

        // 4. Verify followers receive entry and advance commitIndex
        await().atMost(Duration.ofSeconds(3)).until(() -> node2.commitIndex() == 1L && node3.commitIndex() == 1L);
        assertThat(node2.log().lastLogIndex()).isEqualTo(1L);
        assertThat(node3.log().lastLogIndex()).isEqualTo(1L);

        assertThat(new String(node2.log().getEntry(1).get().data(), StandardCharsets.UTF_8))
                .isEqualTo("SET user:1 'Alice'");
        assertThat(new String(node3.log().getEntry(1).get().data(), StandardCharsets.UTF_8))
                .isEqualTo("SET user:1 'Alice'");

        // 5. Propose second write
        byte[] command2 = "SET user:2 'Bob'".getBytes(StandardCharsets.UTF_8);
        CompletableFuture<Long> future2 = node1.propose(command2);
        Long committedIndex2 = future2.get(3, TimeUnit.SECONDS);
        assertThat(committedIndex2).isEqualTo(2L);
        assertThat(node1.commitIndex()).isEqualTo(2L);

        await().atMost(Duration.ofSeconds(3)).until(() -> node2.commitIndex() == 2L && node3.commitIndex() == 2L);

        // 6. Raft Invariants check
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node2.log(), 2L);
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), 2L);
    }
}
