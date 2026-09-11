package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.client.ShardedAegisDbClient;
import se.mouaz.aegisdb.common.*;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import se.mouaz.aegisdb.sharding.*;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Multi-Shard Cluster Integration Test (Master Project Plan §4, §10, §14; US013, US014).
 * Verifies:
 * 1. Data partitioning across discrete Raft consensus groups (US013).
 * 2. Transparent client query routing to shard leaders (US014).
 * 3. Shard fault isolation: Failure of one shard's leader does not disrupt other shards.
 * 4. Transparent client recovery and leader rediscovery upon shard leader re-election.
 */
class MultiShardClusterIntegrationTest {

    private static class ShardCluster {
        final ShardId shardId;
        final List<NodeId> nodeIds = new ArrayList<>();
        final Map<NodeId, InMemoryTransport> transports = new HashMap<>();
        final Map<NodeId, KeyValueStateMachine> stateMachines = new HashMap<>();
        final Map<NodeId, RaftNode> raftNodes = new HashMap<>();

        ShardCluster(ShardId shardId, List<String> nodeNames, int portBase) {
            this.shardId = shardId;
            ClusterConfiguration.Builder cb = ClusterConfiguration.builder().clusterId("cluster-" + shardId);
            for (int i = 0; i < nodeNames.size(); i++) {
                NodeId nid = NodeId.of(nodeNames.get(i));
                nodeIds.add(nid);
                cb.addMember(nid, Endpoint.of("127.0.0.1", portBase + i));
            }
            ClusterConfiguration clusterConfig = cb.build();

            for (int i = 0; i < nodeIds.size(); i++) {
                NodeId nid = nodeIds.get(i);
                InMemoryTransport transport = new InMemoryTransport(nid);
                transports.put(nid, transport);
                KeyValueStateMachine sm = new KeyValueStateMachine();
                stateMachines.put(nid, sm);

                Duration minElection = Duration.ofMillis(60 + (i * 90));
                Duration maxElection = Duration.ofMillis(90 + (i * 90));

                RaftNode node = RaftNode.builder()
                        .nodeId(nid)
                        .clusterConfig(clusterConfig)
                        .transport(transport)
                        .stateMachine(sm)
                        .persistentState(new PersistentRaftState())
                        .minElectionTimeout(minElection)
                        .maxElectionTimeout(maxElection)
                        .heartbeatInterval(Duration.ofMillis(20))
                        .random(new Random(2000 + i + (portBase * 17)))
                        .build();
                raftNodes.put(nid, node);
            }
        }

        void start() {
            transports.values().forEach(InMemoryTransport::start);
            raftNodes.values().forEach(RaftNode::start);
        }

        void stop() {
            raftNodes.values().forEach(RaftNode::stop);
            transports.values().forEach(InMemoryTransport::stop);
        }

        NodeId waitForLeader() {
            await().atMost(5, TimeUnit.SECONDS).until(() ->
                    raftNodes.values().stream().anyMatch(n -> n.role() == RaftRole.LEADER)
            );
            return raftNodes.values().stream()
                    .filter(n -> n.role() == RaftRole.LEADER)
                    .findFirst()
                    .orElseThrow()
                    .nodeId();
        }
    }

    private ShardCluster cluster0;
    private ShardCluster cluster1;
    private ShardManager shardManager;
    private QueryRouter queryRouter;
    private ShardedAegisDbClient client;

    private final Map<NodeId, RaftNode> allNodes = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        ShardId s0 = ShardId.of(0);
        ShardId s1 = ShardId.of(1);

        cluster0 = new ShardCluster(s0, List.of("s0-n1", "s0-n2", "s0-n3"), 17100);
        cluster1 = new ShardCluster(s1, List.of("s1-n1", "s1-n2", "s1-n3"), 17200);

        cluster0.start();
        cluster1.start();

        allNodes.putAll(cluster0.raftNodes);
        allNodes.putAll(cluster1.raftNodes);

        NodeId leader0 = cluster0.waitForLeader();
        NodeId leader1 = cluster1.waitForLeader();

        ShardMap shardMap = new ShardMap();
        shardMap.registerShard(Shard.of(s0, ReplicationGroup.of(s0, new LinkedHashSet<>(cluster0.nodeIds))));
        shardMap.registerShard(Shard.of(s1, ReplicationGroup.of(s1, new LinkedHashSet<>(cluster1.nodeIds))));

        DefaultLeaderLocator leaderLocator = new DefaultLeaderLocator();
        leaderLocator.updateLeader(s0, leader0);
        leaderLocator.updateLeader(s1, leader1);

        shardManager = new ShardManager(shardMap, leaderLocator);

        QueryRouter.ShardNodeInvoker invoker = (shardId, targetNode, command) -> {
            RaftNode node = allNodes.get(targetNode);
            if (node == null) {
                return java.util.concurrent.CompletableFuture.failedFuture(
                        new IllegalArgumentException("Node not found: " + targetNode));
            }
            return node.executeClientCommand(command);
        };

        queryRouter = new QueryRouter(shardManager.router(), shardManager.leaderLocator(), invoker);
        client = new ShardedAegisDbClient(queryRouter);
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.close();
        if (cluster0 != null) cluster0.stop();
        if (cluster1 != null) cluster1.stop();
        InMemoryTransport.clearRegistry();
    }

    @Test
    @DisplayName("Verify keys partition deterministically across distinct Raft groups (US013)")
    void testDataPartitioningAcrossShards() throws Exception {
        HashPartitioner partitioner = new HashPartitioner();
        ShardMap map = shardManager.shardMap();

        String keyForShard0 = null;
        String keyForShard1 = null;

        for (int i = 0; i < 100; i++) {
            String candidate = "key-test-" + i;
            ShardId assigned = partitioner.selectShard(candidate, map);
            if (assigned.equals(ShardId.of(0)) && keyForShard0 == null) {
                keyForShard0 = candidate;
            } else if (assigned.equals(ShardId.of(1)) && keyForShard1 == null) {
                keyForShard1 = candidate;
            }
            if (keyForShard0 != null && keyForShard1 != null) break;
        }

        assertThat(keyForShard0).isNotNull();
        assertThat(keyForShard1).isNotNull();

        client.putString(keyForShard0, "val-0").get(3, TimeUnit.SECONDS);
        client.putString(keyForShard1, "val-1").get(3, TimeUnit.SECONDS);

        // Verify keyForShard0 is replicated in Shard 0 state machines and NOT in Shard 1
        String finalKey0 = keyForShard0;
        await().atMost(3, TimeUnit.SECONDS).until(() ->
                cluster0.stateMachines.values().stream().allMatch(sm -> sm.get(finalKey0) != null)
        );

        for (KeyValueStateMachine sm : cluster1.stateMachines.values()) {
            assertThat(sm.get(keyForShard0)).isNull();
        }

        // Verify keyForShard1 is replicated in Shard 1 state machines and NOT in Shard 0
        String finalKey1 = keyForShard1;
        await().atMost(3, TimeUnit.SECONDS).until(() ->
                cluster1.stateMachines.values().stream().allMatch(sm -> sm.get(finalKey1) != null)
        );

        for (KeyValueStateMachine sm : cluster0.stateMachines.values()) {
            assertThat(sm.get(keyForShard1)).isNull();
        }
    }

    @Test
    @DisplayName("Verify transparent client reads and writes across multiple shards (US014)")
    void testTransparentRoutingAndReads() throws Exception {
        int count = 10;
        for (int i = 0; i < count; i++) {
            client.putString("user:item:" + i, "value-" + i).get(5, TimeUnit.SECONDS);
        }

        for (int i = 0; i < count; i++) {
            Optional<String> val = client.getString("user:item:" + i).get(5, TimeUnit.SECONDS);
            assertThat(val).contains("value-" + i);
        }
    }

    @Test
    @DisplayName("Verify failure of one shard leader does not impact other shards (Fault Isolation)")
    void testShardLeaderFailoverIsolation() throws Exception {
        HashPartitioner partitioner = new HashPartitioner();
        ShardMap map = shardManager.shardMap();

        String keyForShard0 = "key-shard-0";
        while (!partitioner.selectShard(keyForShard0, map).equals(ShardId.of(0))) {
            keyForShard0 = "k-" + UUID.randomUUID();
        }

        String keyForShard1 = "key-shard-1";
        while (!partitioner.selectShard(keyForShard1, map).equals(ShardId.of(1))) {
            keyForShard1 = "k-" + UUID.randomUUID();
        }

        // Both shards functional
        client.putString(keyForShard0, "initial-0").get(3, TimeUnit.SECONDS);
        client.putString(keyForShard1, "initial-1").get(3, TimeUnit.SECONDS);

        // Kill the leader of Shard 0
        NodeId oldLeader0 = shardManager.leaderLocator().getLeader(ShardId.of(0)).orElseThrow();
        cluster0.raftNodes.get(oldLeader0).stop();

        // Shard 1 MUST continue processing writes without any interruption!
        client.putString(keyForShard1, "updated-1-during-shard0-failover").get(3, TimeUnit.SECONDS);
        assertThat(client.getString(keyForShard1).get(2, TimeUnit.SECONDS))
                .contains("updated-1-during-shard0-failover");

        // Wait for Shard 0 to elect a new leader among remaining 2 nodes
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                cluster0.raftNodes.values().stream()
                        .filter(n -> !n.nodeId().equals(oldLeader0))
                        .anyMatch(n -> n.role() == RaftRole.LEADER)
        );

        // Client write to Shard 0 now succeeds via transparent leader failover and retry!
        client.putString(keyForShard0, "recovered-0").get(5, TimeUnit.SECONDS);
        assertThat(client.getString(keyForShard0).get(2, TimeUnit.SECONDS))
                .contains("recovered-0");
    }
}
