package se.mouaz.aegisdb.sharding;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HashPartitionerTest {

    private HashPartitioner partitioner;
    private ShardMap shardMap;

    @BeforeEach
    void setUp() {
        partitioner = new HashPartitioner();
        shardMap = new ShardMap();
        for (int i = 0; i < 4; i++) {
            ShardId id = ShardId.of(i);
            ReplicationGroup group = ReplicationGroup.of(id, NodeId.of("node-" + (i + 1)));
            shardMap.registerShard(Shard.of(id, group));
        }
    }

    @Test
    void testDeterminismInvariant() {
        String key = "customer:order:987654";
        ShardId expected = partitioner.selectShard(key, shardMap);

        // Determinism: Same key must always map to same shard across 1,000 queries
        for (int i = 0; i < 1000; i++) {
            assertThat(partitioner.selectShard(key, shardMap)).isEqualTo(expected);
        }
    }

    @Test
    void testNegativeHashHandlingWithFloorMod() {
        // Find keys that produce negative 32-bit Murmur3 hash values
        int negativeHashCount = 0;
        for (int i = 0; i < 100; i++) {
            String key = "test-key-" + i;
            int hash = partitioner.hash(key);
            if (hash < 0) {
                negativeHashCount++;
                int index = partitioner.calculateShardIndex(key, 4);
                assertThat(index).isBetween(0, 3);
            }
        }
        assertThat(negativeHashCount).isGreaterThan(10);
    }

    @Test
    void testUniformDistributionAcrossShards() {
        int shardCount = 5;
        ShardMap fiveShardMap = new ShardMap();
        for (int i = 0; i < shardCount; i++) {
            ShardId id = ShardId.of(i);
            fiveShardMap.registerShard(Shard.of(id, ReplicationGroup.of(id, NodeId.of("node-" + i))));
        }

        int totalKeys = 50_000;
        int expectedPerShard = totalKeys / shardCount; // 10,000 per shard
        Map<ShardId, Integer> distribution = new HashMap<>();

        for (int i = 0; i < totalKeys; i++) {
            String key = "key:prefix:item_" + i + "_uuid_" + UUID.nameUUIDFromBytes(("seed" + i).getBytes());
            ShardId assigned = partitioner.selectShard(key, fiveShardMap);
            distribution.merge(assigned, 1, Integer::sum);
        }

        assertThat(distribution).hasSize(shardCount);

        // Every shard should be within +/- 10% of expected distribution
        double maxDeviation = 0.10;
        int minExpected = (int) (expectedPerShard * (1.0 - maxDeviation));
        int maxExpected = (int) (expectedPerShard * (1.0 + maxDeviation));

        for (int i = 0; i < shardCount; i++) {
            int count = distribution.getOrDefault(ShardId.of(i), 0);
            assertThat(count)
                    .as("Shard %d count (%d) within expected range [%d, %d]", i, count, minExpected, maxExpected)
                    .isBetween(minExpected, maxExpected);
        }
    }

    @Test
    void testSingleShardDegenerateCase() {
        ShardMap singleMap = new ShardMap();
        ShardId s0 = ShardId.of(0);
        singleMap.registerShard(Shard.of(s0, ReplicationGroup.of(s0, NodeId.of("node-1"))));

        for (int i = 0; i < 100; i++) {
            assertThat(partitioner.selectShard("key-" + i, singleMap)).isEqualTo(s0);
        }
    }

    @Test
    void testEdgeCasesAndValidation() {
        assertThatThrownBy(() -> partitioner.selectShard(null, shardMap))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> partitioner.selectShard("key", null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> partitioner.selectShard("key", new ShardMap()))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> partitioner.calculateShardIndex("key", 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
