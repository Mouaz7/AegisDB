package se.mouaz.aegisdb.sharding;

import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShardMapTest {

    @Test
    void testRegisterAndRetrieveShards() {
        ShardMap map = new ShardMap();
        assertThat(map.shardCount()).isZero();

        ShardId s0 = ShardId.of(0);
        ShardId s1 = ShardId.of(1);
        Shard shard0 = Shard.of(s0, ReplicationGroup.of(s0, NodeId.of("node-1")));
        Shard shard1 = Shard.of(s1, ReplicationGroup.of(s1, NodeId.of("node-2")));

        map.registerShard(shard0);
        map.registerShard(shard1);

        assertThat(map.shardCount()).isEqualTo(2);
        assertThat(map.containsShard(s0)).isTrue();
        assertThat(map.containsShard(s1)).isTrue();
        assertThat(map.getShard(s0)).contains(shard0);
        assertThat(map.getShard(s1)).contains(shard1);

        assertThat(map.getShardByIndex(0)).contains(shard0);
        assertThat(map.getShardByIndex(1)).contains(shard1);
        assertThat(map.getShardByIndex(2)).isEmpty();
    }

    @Test
    void testRemoveShard() {
        ShardMap map = new ShardMap();
        ShardId s0 = ShardId.of(0);
        Shard shard0 = Shard.of(s0, ReplicationGroup.of(s0, NodeId.of("node-1")));

        map.registerShard(shard0);
        assertThat(map.shardCount()).isEqualTo(1);

        Optional<Shard> removed = map.removeShard(s0);
        assertThat(removed).contains(shard0);
        assertThat(map.shardCount()).isZero();
        assertThat(map.containsShard(s0)).isFalse();
    }

    @Test
    void testImmutabilityOfAllShards() {
        ShardMap map = new ShardMap();
        ShardId s0 = ShardId.of(0);
        map.registerShard(Shard.of(s0, ReplicationGroup.of(s0, NodeId.of("node-1"))));

        List<Shard> snapshot = map.allShards();
        assertThat(snapshot).hasSize(1);

        assertThatThrownBy(() -> snapshot.add(Shard.of(ShardId.of(1), ReplicationGroup.of(ShardId.of(1), NodeId.of("node-2")))))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testConcurrentRegistrations() throws Exception {
        ShardMap map = new ShardMap();
        int threads = 8;
        int shardsPerThread = 25;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            final int threadIdx = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < shardsPerThread; i++) {
                        int shardIdx = threadIdx * shardsPerThread + i;
                        ShardId id = ShardId.of(shardIdx);
                        map.registerShard(Shard.of(id, ReplicationGroup.of(id, NodeId.of("node-" + (shardIdx % 5)))));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        executor.shutdownNow();

        assertThat(map.shardCount()).isEqualTo(threads * shardsPerThread);
    }
}
