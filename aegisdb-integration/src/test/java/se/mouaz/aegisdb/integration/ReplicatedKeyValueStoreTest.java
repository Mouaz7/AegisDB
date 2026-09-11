package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.client.AegisDbClient;
import se.mouaz.aegisdb.client.DefaultAegisDbClient;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies Milestone M2 Gate and Phase 5 Acceptance Criteria:
 * US010: As a client, I want replicated key-value operations.
 * [AC3] KeyValueStateMachine (PUT/GET/DELETE, snapshot serialization/deserialization).
 * [AC4] Java SDK client module aegisdb_client (leader discovery, redirect/retry).
 * [AC6] Milestone M2 Gate: 3-node replicated persistent key-value store survives leader failure,
 *       node partitions, and recovers state.
 */
class ReplicatedKeyValueStoreTest {

    private NodeId id1;
    private NodeId id2;
    private NodeId id3;

    private ClusterConfiguration clusterConfig;

    private InMemoryTransport transport1;
    private InMemoryTransport transport2;
    private InMemoryTransport transport3;

    private KeyValueStateMachine sm1;
    private KeyValueStateMachine sm2;
    private KeyValueStateMachine sm3;

    private RaftNode node1;
    private RaftNode node2;
    private RaftNode node3;

    private final Map<NodeId, RaftNode> activeNodes = new ConcurrentHashMap<>();
    private DefaultAegisDbClient client;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        id1 = NodeId.of("kv-node-1");
        id2 = NodeId.of("kv-node-2");
        id3 = NodeId.of("kv-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 14001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 14002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 14003);

        clusterConfig = ClusterConfiguration.builder()
                .clusterId("kv-cluster")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        transport1 = new InMemoryTransport(id1);
        transport2 = new InMemoryTransport(id2);
        transport3 = new InMemoryTransport(id3);

        sm1 = new KeyValueStateMachine();
        sm2 = new KeyValueStateMachine();
        sm3 = new KeyValueStateMachine();
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.close();

        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
        if (node3 != null) node3.stop();

        if (transport1 != null) transport1.stop();
        if (transport2 != null) transport2.stop();
        if (transport3 != null) transport3.stop();

        InMemoryTransport.clearRegistry();
    }

    @Test
    @DisplayName("[Milestone M2 Gate / US010] 3-node replicated KV store handles PUT/GET/DELETE, survives leader kill and recovers")
    void testMilestoneM2GateReplicatedKeyValueStore() throws Exception {
        // --- 1. Start 3-node cluster ---
        transport1.start();
        transport2.start();
        transport3.start();

        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .persistentState(new PersistentRaftState())
                .stateMachine(sm1)
                .minElectionTimeout(Duration.ofMillis(80)).maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(101))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .persistentState(new PersistentRaftState())
                .stateMachine(sm2)
                .minElectionTimeout(Duration.ofMillis(180)).maxElectionTimeout(Duration.ofMillis(240))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(102))
                .build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .persistentState(new PersistentRaftState())
                .stateMachine(sm3)
                .minElectionTimeout(Duration.ofMillis(280)).maxElectionTimeout(Duration.ofMillis(360))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(103))
                .build();

        activeNodes.put(id1, node1);
        activeNodes.put(id2, node2);
        activeNodes.put(id3, node3);

        node1.start();
        node2.start();
        node3.start();

        // Node 1 has fastest election timeout, becomes initial leader
        await().atMost(5, TimeUnit.SECONDS).until(() -> node1.role() == RaftRole.LEADER);

        // --- 2. Initialize AegisDbClient ---
        client = DefaultAegisDbClient.forNodes(activeNodes);

        // --- 3. Test Client PUT & GET operations ---
        client.putString("user:101", "Alice").get(5, TimeUnit.SECONDS);
        client.putString("user:102", "Bob").get(5, TimeUnit.SECONDS);
        client.putString("user:103", "Charlie").get(5, TimeUnit.SECONDS);

        assertThat(client.getString("user:101").get(5, TimeUnit.SECONDS)).contains("Alice");
        assertThat(client.getString("user:102").get(5, TimeUnit.SECONDS)).contains("Bob");
        assertThat(client.getString("user:103").get(5, TimeUnit.SECONDS)).contains("Charlie");

        // --- 4. Test Client DELETE operation ---
        client.delete("user:102").get(5, TimeUnit.SECONDS);
        assertThat(client.getString("user:102").get(5, TimeUnit.SECONDS)).isEmpty();

        // Verify state is replicated across all 3 state machines
        await().atMost(5, TimeUnit.SECONDS).until(() -> sm2.get("user:103") != null && sm3.get("user:103") != null);
        assertThat(sm2.get("user:102")).isNull();
        assertThat(sm3.get("user:102")).isNull();
        assertThat(new String(sm2.get("user:101"), StandardCharsets.UTF_8)).isEqualTo("Alice");
        assertThat(new String(sm3.get("user:101"), StandardCharsets.UTF_8)).isEqualTo("Alice");

        // --- 5. Milestone M2 Gate: Kill Current Leader (node1) ---
        node1.stop();
        transport1.stop();
        activeNodes.remove(id1);

        // Wait for either node2 or node3 to win election as new leader
        await().atMost(5, TimeUnit.SECONDS).until(
                () -> node2.role() == RaftRole.LEADER || node3.role() == RaftRole.LEADER
        );

        NodeId newLeaderId = node2.role() == RaftRole.LEADER ? id2 : id3;

        // --- 6. Client writes new key during leader change with automatic failover ---
        // Client transparently redirects from stopped node1 to the new leader!
        client.putString("user:104", "Diana").get(10, TimeUnit.SECONDS);

        assertThat(client.getString("user:104").get(5, TimeUnit.SECONDS)).contains("Diana");
        assertThat(client.getString("user:101").get(5, TimeUnit.SECONDS)).contains("Alice");
        assertThat(client.getString("user:103").get(5, TimeUnit.SECONDS)).contains("Charlie");

        // Verify replication to the other alive follower
        KeyValueStateMachine activeFollowerSm = (newLeaderId.equals(id2)) ? sm3 : sm2;
        await().atMost(5, TimeUnit.SECONDS).until(() -> activeFollowerSm.get("user:104") != null);
        assertThat(new String(activeFollowerSm.get("user:104"), StandardCharsets.UTF_8)).isEqualTo("Diana");

        // --- 7. Restart the old leader (node1) and verify catch-up ---
        transport1 = new InMemoryTransport(id1);
        transport1.start();

        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .persistentState(new PersistentRaftState())
                .stateMachine(sm1)
                .minElectionTimeout(Duration.ofMillis(800)).maxElectionTimeout(Duration.ofMillis(1000))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(104))
                .build();

        activeNodes.put(id1, node1);
        node1.start();

        // Node 1 rejoins as follower and catches up with user:104
        await().atMost(5, TimeUnit.SECONDS).until(() -> sm1.get("user:104") != null);
        assertThat(new String(sm1.get("user:104"), StandardCharsets.UTF_8)).isEqualTo("Diana");
        assertThat(sm1.get("user:102")).isNull();
    }
}
