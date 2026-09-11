package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ShardId;

import java.util.Objects;

/**
 * Deterministic hash-based partitioner implementing:
 * shard = floorMod(Murmur3.hash32(key), shardCount)
 * (Master Project Plan §10; US013, US014).
 *
 * Guarantees uniform key dispersion across shards, zero clustering,
 * deterministic cross-platform behavior, and safe negative-hash handling.
 */
public class HashPartitioner implements Partitioner {

    private final int seed;

    public HashPartitioner() {
        this(0x9747b28c);
    }

    public HashPartitioner(int seed) {
        this.seed = seed;
    }

    @Override
    public ShardId selectShard(String key, ShardMap shardMap) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(shardMap, "shardMap cannot be null");

        int count = shardMap.shardCount();
        if (count == 0) {
            throw new IllegalStateException("Cannot partition key '" + key + "' on empty ShardMap");
        }

        int shardIndex = calculateShardIndex(key, count);
        return shardMap.getShardByIndex(shardIndex)
                .map(Shard::id)
                .orElseThrow(() -> new IllegalStateException("Shard index " + shardIndex + " not found in ShardMap"));
    }

    /**
     * Calculates the zero-based shard index for a key given the shard count.
     */
    public int calculateShardIndex(String key, int shardCount) {
        Objects.requireNonNull(key, "key cannot be null");
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be positive, got: " + shardCount);
        }
        int hash = Murmur3.hash32(key, seed);
        return Math.floorMod(hash, shardCount);
    }

    /**
     * Computes the 32-bit MurmurHash3 for a key.
     */
    public int hash(String key) {
        return Murmur3.hash32(key, seed);
    }

    public int seed() {
        return seed;
    }
}
