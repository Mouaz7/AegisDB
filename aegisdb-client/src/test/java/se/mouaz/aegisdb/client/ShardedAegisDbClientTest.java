package se.mouaz.aegisdb.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.raft.statemachine.KvCommand;
import se.mouaz.aegisdb.sharding.*;
import se.mouaz.aegisdb.transaction.IsolationLevel;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShardedAegisDbClientTest {

    private ShardManager shardManager;
    private QueryRouter queryRouter;
    private ShardedAegisDbClient client;
    private final Map<ShardId, Map<String, byte[]>> simulatedShardStorage = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        simulatedShardStorage.clear();

        List<NodeId> nodes = List.of(
                NodeId.of("node-1"),
                NodeId.of("node-2"),
                NodeId.of("node-3"),
                NodeId.of("node-4")
        );

        // Create 3 shards across 4 nodes with replication factor 2
        shardManager = ShardManager.createStaticShards(3, nodes, 2);

        QueryRouter.ShardNodeInvoker invoker = (shardId, targetNode, cmdBytes) -> {
            KvCommand cmd = KvCommand.fromBytes(cmdBytes);
            Map<String, byte[]> store = simulatedShardStorage.computeIfAbsent(shardId, k -> new ConcurrentHashMap<>());

            switch (cmd.opType()) {
                case PUT -> {
                    store.put(cmd.key(), cmd.value());
                    return CompletableFuture.completedFuture(new byte[0]);
                }
                case GET -> {
                    byte[] val = store.get(cmd.key());
                    return CompletableFuture.completedFuture(val != null ? val : new byte[0]);
                }
                case DELETE -> {
                    byte[] val = store.remove(cmd.key());
                    return CompletableFuture.completedFuture(val != null ? val : new byte[0]);
                }
                default -> {
                    return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown cmd: " + cmd.opType()));
                }
            }
        };

        queryRouter = new QueryRouter(shardManager.router(), shardManager.leaderLocator(), invoker);
        client = new ShardedAegisDbClient(queryRouter);
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void testTransparentMultiShardOperations() throws Exception {
        int totalKeys = 300;
        for (int i = 0; i < totalKeys; i++) {
            client.putString("user:profile:" + i, "data-" + i).get();
        }

        // Verify keys are partitioned across all 3 shards
        assertThat(simulatedShardStorage).hasSize(3);
        int totalStored = simulatedShardStorage.values().stream().mapToInt(Map::size).sum();
        assertThat(totalStored).isEqualTo(totalKeys);

        // Verify each individual shard holds data
        for (Shard shard : shardManager.shardMap().allShards()) {
            Map<String, byte[]> shardData = simulatedShardStorage.get(shard.id());
            assertThat(shardData).isNotNull();
            assertThat(shardData).isNotEmpty();
        }

        // Verify transparent GET and DELETE
        Optional<String> val = client.getString("user:profile:100").get();
        assertThat(val).contains("data-100");

        Optional<byte[]> deleted = client.delete("user:profile:100").get();
        assertThat(deleted).isPresent();

        Optional<String> valAfterDelete = client.getString("user:profile:100").get();
        assertThat(valAfterDelete).isEmpty();
    }

    @Test
    void testShardResolutionAndLeaderTracking() {
        String key = "account:checking:999";
        ShardId shardId = client.resolveShard(key);
        assertThat(shardId).isNotNull();

        Optional<NodeId> keyLeader = client.currentLeader(key);
        Optional<NodeId> shardLeader = client.currentLeader(shardId);

        assertThat(keyLeader).isPresent();
        assertThat(shardLeader).isPresent();
        assertThat(keyLeader).isEqualTo(shardLeader);
    }

    @Test
    void testTransactionGuardForMultiShardCluster() {
        // Multi-shard cluster requires explicit shard targeting or 2PC
        assertThatThrownBy(() -> client.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Multi-shard cluster requires explicit shard targeting");
    }
}
