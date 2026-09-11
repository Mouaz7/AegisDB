package se.mouaz.aegisdb.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.raft.statemachine.KvCommand;
import se.mouaz.aegisdb.sharding.QueryRouter;
import se.mouaz.aegisdb.sharding.ShardManager;
import se.mouaz.aegisdb.transaction.IsolationLevel;
import se.mouaz.aegisdb.transaction.Transaction;
import se.mouaz.aegisdb.transaction.TransactionManager;
import se.mouaz.aegisdb.transaction.distributed.InMemoryCoordinatorLog;
import se.mouaz.aegisdb.transaction.distributed.LocalShardParticipant;
import se.mouaz.aegisdb.transaction.distributed.TransactionParticipant;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class ShardedAegisDbClientDistributedTransactionTest {

    private ShardId shard0;
    private ShardId shard1;
    private MvccStore store0;
    private MvccStore store1;
    private TransactionManager tm0;
    private TransactionManager tm1;
    private LocalShardParticipant participant0;
    private LocalShardParticipant participant1;
    private ShardedAegisDbClient client;

    @BeforeEach
    void setUp() {
        shard0 = ShardId.of("shard-0");
        shard1 = ShardId.of("shard-1");

        store0 = new MvccStore();
        store1 = new MvccStore();

        tm0 = new TransactionManager(store0);
        tm1 = new TransactionManager(store1);

        participant0 = new LocalShardParticipant(shard0, tm0);
        participant1 = new LocalShardParticipant(shard1, tm1);

        Map<ShardId, TransactionParticipant> participants = Map.of(
                shard0, participant0,
                shard1, participant1
        );

        List<NodeId> nodes = List.of(NodeId.of("node-1"), NodeId.of("node-2"));
        ShardManager shardManager = ShardManager.createStaticShards(2, nodes, 1);

        QueryRouter.ShardNodeInvoker invoker = (sId, targetNode, cmdBytes) -> {
            KvCommand cmd = KvCommand.fromBytes(cmdBytes);
            MvccStore targetStore = sId.equals(shard0) ? store0 : store1;
            return switch (cmd.opType()) {
                case PUT -> {
                    targetStore.put(cmd.key(), cmd.value());
                    yield CompletableFuture.completedFuture(new byte[0]);
                }
                case GET -> {
                    byte[] val = targetStore.get(cmd.key()).orElse(null);
                    yield CompletableFuture.completedFuture(val != null ? val : new byte[0]);
                }
                case DELETE -> {
                    targetStore.delete(cmd.key());
                    yield CompletableFuture.completedFuture(new byte[0]);
                }
                default -> CompletableFuture.failedFuture(new IllegalArgumentException("Unknown cmd"));
            };
        };

        QueryRouter queryRouter = new QueryRouter(shardManager.router(), shardManager.leaderLocator(), invoker);

        client = new ShardedAegisDbClient(
                queryRouter,
                Collections.emptyMap(),
                participants,
                new InMemoryCoordinatorLog()
        );
    }

    @AfterEach
    void tearDown() {
        client.close();
    }

    @Test
    void testCrossShardDistributedTransactionCommit() {
        String keyOnShard0 = findKeyForShard(shard0);
        String keyOnShard1 = findKeyForShard(shard1);

        try (Transaction tx = client.beginTransaction(IsolationLevel.SERIALIZABLE)) {
            tx.putString(keyOnShard0, "val0");
            tx.putString(keyOnShard1, "val1");

            // Read-your-own-writes from local buffer
            assertThat(tx.getString(keyOnShard0)).contains("val0");
            assertThat(tx.getString(keyOnShard1)).contains("val1");

            tx.commit();
        }

        // Verify committed on both shard stores
        assertThat(store0.get(keyOnShard0)).isPresent();
        assertThat(new String(store0.get(keyOnShard0).get(), StandardCharsets.UTF_8)).isEqualTo("val0");

        assertThat(store1.get(keyOnShard1)).isPresent();
        assertThat(new String(store1.get(keyOnShard1).get(), StandardCharsets.UTF_8)).isEqualTo("val1");
    }

    @Test
    void testCrossShardDistributedTransactionAbort() {
        String keyOnShard0 = findKeyForShard(shard0);
        String keyOnShard1 = findKeyForShard(shard1);

        try (Transaction tx = client.beginTransaction(IsolationLevel.SERIALIZABLE)) {
            tx.putString(keyOnShard0, "val0");
            tx.putString(keyOnShard1, "val1");
            tx.abort();
        }

        // Verify neither was committed and locks released
        assertThat(store0.get(keyOnShard0)).isEmpty();
        assertThat(store1.get(keyOnShard1)).isEmpty();
        assertThat(participant0.isKeyLocked(keyOnShard0)).isFalse();
        assertThat(participant1.isKeyLocked(keyOnShard1)).isFalse();
    }

    @Test
    void testRunInTransaction() {
        String keyOnShard0 = findKeyForShard(shard0);
        String keyOnShard1 = findKeyForShard(shard1);

        String result = client.runInTransaction(IsolationLevel.SERIALIZABLE, tx -> {
            tx.putString(keyOnShard0, "auto0");
            tx.putString(keyOnShard1, "auto1");
            return "SUCCESS";
        }, 3);

        assertThat(result).isEqualTo("SUCCESS");
        assertThat(store0.get(keyOnShard0)).isPresent();
        assertThat(store1.get(keyOnShard1)).isPresent();
    }

    private String findKeyForShard(ShardId targetShard) {
        for (int i = 0; i < 1000; i++) {
            String key = "test-key-" + i;
            if (client.resolveShard(key).equals(targetShard)) {
                return key;
            }
        }
        throw new IllegalStateException("Could not find key routing to shard: " + targetShard);
    }
}
