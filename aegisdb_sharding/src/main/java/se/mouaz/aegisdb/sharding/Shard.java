package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ShardId;

import java.util.Objects;

/**
 * Represents a logical database shard composed of its identity, metadata, and Raft replication group (Master Project Plan §10; US013).
 */
public record Shard(ShardId id, ShardMetadata metadata, ReplicationGroup replicationGroup) {

    public Shard {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(metadata, "metadata cannot be null");
        Objects.requireNonNull(replicationGroup, "replicationGroup cannot be null");
        if (!id.equals(metadata.shardId())) {
            throw new IllegalArgumentException("Shard id mismatch: " + id + " vs metadata " + metadata.shardId());
        }
        if (!id.equals(replicationGroup.shardId())) {
            throw new IllegalArgumentException("Shard id mismatch: " + id + " vs replicationGroup " + replicationGroup.shardId());
        }
    }

    public static Shard of(ShardId id, ReplicationGroup replicationGroup) {
        Objects.requireNonNull(id, "id cannot be null");
        Objects.requireNonNull(replicationGroup, "replicationGroup cannot be null");
        ShardMetadata meta = ShardMetadata.active(id, replicationGroup.size());
        return new Shard(id, meta, replicationGroup);
    }
}
