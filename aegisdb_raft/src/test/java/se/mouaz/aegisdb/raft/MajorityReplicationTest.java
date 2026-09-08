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

@DisplayName("Majority Replication Test - Acceptance Criterion 2 (US006)")
class MajorityReplicationTest {

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

        id1 = NodeId.of("maj-node-1");
        id2 = NodeId.of("maj-node-2");
        id3 = NodeId.of("maj-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 7201);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 7202);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 7203);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("maj-cluster")
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
    @DisplayName("Write does not commit without majority, commits once majority is restored")
    void majorityRequiredForCommit() throws Exception {
        // 1. Wait for node 1 to be leader
        await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);

        // 2. Disconnect both followers (transport2 and transport3 stopped)
        transport2.stop();
        transport3.stop();

        // 3. Propose write while partitioned from majority
        byte[] command = "CRITICAL_PAYMENT".getBytes(StandardCharsets.UTF_8);
        CompletableFuture<Long> future = node1.propose(command);

        // Leader has written entry to local log via event loop, but cannot commit without majority
        await().atMost(Duration.ofSeconds(2)).until(() -> node1.log().lastLogIndex() == 1L);
        assertThat(node1.commitIndex()).isZero();

        // Wait to verify it does NOT complete
        Thread.sleep(300);
        assertThat(future.isDone()).isFalse();
        assertThat(node1.commitIndex()).isZero();

        // 4. Restore node 2 back into network (now 2 out of 3 = majority)
        transport2.start();
        node1.replicationManager().replicateTo(id2);

        // 5. Future must now complete and commitIndex must advance!
        Long commit = future.get(3, TimeUnit.SECONDS);
        assertThat(commit).isEqualTo(1L);
        assertThat(node1.commitIndex()).isEqualTo(1L);

        await().atMost(Duration.ofSeconds(3)).until(() -> node2.commitIndex() == 1L);
        assertThat(node2.log().lastLogIndex()).isEqualTo(1L);
    }
}
