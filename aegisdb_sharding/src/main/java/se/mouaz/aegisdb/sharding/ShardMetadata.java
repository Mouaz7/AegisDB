package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ShardId;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata descriptor for a database shard partition (Master Project Plan §10; US013).
 */
public record ShardMetadata(
        ShardId shardId,
        Instant createdAt,
        int replicaCount,
        ShardStatus status,
        Map<String, String> attributes) {

    public enum ShardStatus {
        ACTIVE,
        REBALANCING,
        SPLITTING,
        READ_ONLY,
        DECOMMISSIONED
    }

    public ShardMetadata {
        Objects.requireNonNull(shardId, "shardId cannot be null");
        Objects.requireNonNull(createdAt, "createdAt cannot be null");
        Objects.requireNonNull(status, "status cannot be null");
        if (replicaCount < 1) {
            throw new IllegalArgumentException("replicaCount must be positive, got: " + replicaCount);
        }
        attributes = attributes != null ? Collections.unmodifiableMap(new HashMap<>(attributes)) : Collections.emptyMap();
    }

    public static ShardMetadata active(ShardId shardId, int replicaCount) {
        return new ShardMetadata(shardId, Instant.now(), replicaCount, ShardStatus.ACTIVE, Collections.emptyMap());
    }

    public static ShardMetadata active(ShardId shardId, int replicaCount, Map<String, String> attributes) {
        return new ShardMetadata(shardId, Instant.now(), replicaCount, ShardStatus.ACTIVE, attributes);
    }
}
