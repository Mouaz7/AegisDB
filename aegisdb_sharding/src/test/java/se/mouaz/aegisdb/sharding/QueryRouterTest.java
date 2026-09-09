package se.mouaz.aegisdb.sharding;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.raft.NotLeaderException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class QueryRouterTest {

    private ShardMap shardMap;
    private DefaultLeaderLocator leaderLocator;
    private ShardRouter shardRouter;
    private ScheduledExecutorService scheduler;
    private QueryRouter queryRouter;

    private final NodeId node1 = NodeId.of("node-1");
    private final NodeId node2 = NodeId.of("node-2");
    private final ShardId shard0 = ShardId.of(0);

    @BeforeEach
    void setUp() {
        shardMap = new ShardMap();
        ReplicationGroup group = ReplicationGroup.of(shard0, node1, node2);
        shardMap.registerShard(Shard.of(shard0, group));

        leaderLocator = new DefaultLeaderLocator();
        shardRouter = new ShardRouter(shardMap);
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        if (queryRouter != null) {
            queryRouter.close();
        }
        scheduler.shutdownNow();
    }

    @Test
    void testSuccessfulDirectRouteToCachedLeader() throws Exception {
        leaderLocator.updateLeader(shard0, node1);

        QueryRouter.ShardNodeInvoker invoker = (shardId, targetNode, cmd) -> {
            assertThat(targetNode).isEqualTo(node1);
            return CompletableFuture.completedFuture("OK".getBytes(StandardCharsets.UTF_8));
        };

        queryRouter = new QueryRouter(shardRouter, leaderLocator, invoker, 3, Duration.ofMillis(10), Duration.ofMillis(50), scheduler);

        byte[] response = queryRouter.execute("key-1", "cmd".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        assertThat(new String(response, StandardCharsets.UTF_8)).isEqualTo("OK");
    }

    @Test
    void testFailoverWithLeaderHintRedirect() throws Exception {
        // Initially point to node1, which is NOT the leader and provides hint -> node2
        leaderLocator.updateLeader(shard0, node1);
        AtomicInteger attempts = new AtomicInteger(0);

        QueryRouter.ShardNodeInvoker invoker = (shardId, targetNode, cmd) -> {
            int attempt = attempts.incrementAndGet();
            if (targetNode.equals(node1)) {
                return CompletableFuture.failedFuture(new NotLeaderException(node2, 2L));
            } else if (targetNode.equals(node2)) {
                return CompletableFuture.completedFuture("REDIRECT_SUCCESS".getBytes(StandardCharsets.UTF_8));
            }
            return CompletableFuture.failedFuture(new IllegalStateException("Unknown node: " + targetNode));
        };

        queryRouter = new QueryRouter(shardRouter, leaderLocator, invoker, 3, Duration.ofMillis(10), Duration.ofMillis(50), scheduler);

        byte[] response = queryRouter.execute("key-1", "cmd".getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
        assertThat(new String(response, StandardCharsets.UTF_8)).isEqualTo("REDIRECT_SUCCESS");
        assertThat(attempts.get()).isEqualTo(2);

        // Leader locator cache must now point to node2!
        assertThat(leaderLocator.getLeader(shard0)).contains(node2);
    }

    @Test
    void testFailoverWithoutLeaderHintRoundRobin() throws Exception {
        // Point to node1, which fails without a hint
        leaderLocator.updateLeader(shard0, node1);
        AtomicInteger attempts = new AtomicInteger(0);

        QueryRouter.ShardNodeInvoker invoker = (shardId, targetNode, cmd) -> {
            attempts.incrementAndGet();
            if (targetNode.equals(node1)) {
                return CompletableFuture.failedFuture(new NotLeaderException(null, 1L));
            } else if (targetNode.equals(node2)) {
                return CompletableFuture.completedFuture("ROUND_ROBIN_SUCCESS".getBytes(StandardCharsets.UTF_8));
            }
            return CompletableFuture.failedFuture(new IllegalStateException("Unknown node: " + targetNode));
        };

        queryRouter = new QueryRouter(shardRouter, leaderLocator, invoker, 3, Duration.ofMillis(10), Duration.ofMillis(50), scheduler);

        byte[] response = queryRouter.execute("key-1", "cmd".getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
        assertThat(new String(response, StandardCharsets.UTF_8)).isEqualTo("ROUND_ROBIN_SUCCESS");

        // Leader locator cache must now point to node2!
        assertThat(leaderLocator.getLeader(shard0)).contains(node2);
    }
}
