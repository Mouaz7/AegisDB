package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ShardId;

import java.util.Objects;

/**
 * Shard router that resolves any key to its corresponding destination Shard and ReplicationGroup (Master Project Plan §10; US013, US014).
 * Combines the configured Partitioner and active ShardMap.
 */
public class ShardRouter {

    private final Partitioner partitioner;
    private final ShardMap shardMap;

    public ShardRouter(Partitioner partitioner, ShardMap shardMap) {
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner cannot be null");
        this.shardMap = Objects.requireNonNull(shardMap, "shardMap cannot be null");
    }

    public ShardRouter(ShardMap shardMap) {
        this(new HashPartitioner(), shardMap);
    }

    /**
     * Resolves the target Shard for the specified key.
     */
    public Shard route(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        ShardId shardId = partitioner.selectShard(key, shardMap);
        return shardMap.getShard(shardId)
                .orElseThrow(() -> new IllegalStateException("Shard " + shardId + " not found in ShardMap"));
    }

    /**
     * Resolves the target ShardId for the specified key.
     */
    public ShardId routeToShardId(String key) {
        return partitioner.selectShard(key, shardMap);
    }

    /**
     * Resolves the target ReplicationGroup for the specified key.
     */
    public ReplicationGroup routeToReplicationGroup(String key) {
        return route(key).replicationGroup();
    }

    public Partitioner partitioner() {
        return partitioner;
    }

    public ShardMap shardMap() {
        return shardMap;
    }
}
