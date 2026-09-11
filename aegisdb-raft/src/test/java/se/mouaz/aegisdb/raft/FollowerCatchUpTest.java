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

@DisplayName("Follower Catch Up Test - Acceptance Criterion 4 (US006)")
class FollowerCatchUpTest {

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

        id1 = NodeId.of("catch-node-1");
        id2 = NodeId.of("catch-node-2");
        id3 = NodeId.of("catch-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 7301);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 7302);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 7303);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("catch-cluster")
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

        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(80))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(42))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(600))
                .maxElectionTimeout(Duration.ofMillis(800))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(43))
                .build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(600))
                .maxElectionTimeout(Duration.ofMillis(800))
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
    @DisplayName("Lagging follower catches up after reconnecting to leader")
    void laggingFollowerCatchesUp() throws Exception {
        // 1. Wait for leader election
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);

        // 2. Disconnect Node 3 before writes
        transport3.stop();

        // 3. Replicate 5 writes between Node 1 and Node 2
        for (int i = 1; i <= 5; i++) {
            CompletableFuture<Long> fut = node1.propose(("cmd-" + i).getBytes(StandardCharsets.UTF_8));
            Long committed = fut.get(3, TimeUnit.SECONDS);
            assertThat(committed).isEqualTo((long) i);
        }

        assertThat(node1.commitIndex()).isEqualTo(5L);
        await().atMost(Duration.ofSeconds(2)).until(() -> node2.commitIndex() == 5L);

        // Node 3 is still empty
        assertThat(node3.log().lastLogIndex()).isZero();
        assertThat(node3.commitIndex()).isZero();

        // 4. Reconnect Node 3
        transport3.start();

        // Trigger replication from leader to node 3
        node1.replicationManager().replicateTo(id3);

        // 5. Node 3 catches up to index 5 and advances commitIndex to 5!
        await().atMost(Duration.ofSeconds(4)).until(() -> node3.commitIndex() == 5L);
        assertThat(node3.log().lastLogIndex()).isEqualTo(5L);

        for (int i = 1; i <= 5; i++) {
            assertThat(node3.log().getEntry(i)).isPresent();
            assertThat(new String(node3.log().getEntry(i).get().data(), StandardCharsets.UTF_8))
                    .isEqualTo("cmd-" + i);
        }

        // 6. Raft Invariants check
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), 5L);
    }
}
