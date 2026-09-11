package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ShardId;

/**
 * Strategy interface for mapping keys to shards (Master Project Plan §10; US013, US014).
 */
@FunctionalInterface
public interface Partitioner {

    /**
     * Determines the destination ShardId for a given key within the current shard topology.
     *
     * @param key      the database key to partition
     * @param shardMap the current shard topology map
     * @return the destination ShardId
     * @throws IllegalStateException if shardMap is empty
     */
    ShardId selectShard(String key, ShardMap shardMap);
}
