package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.ShardLifecycle;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Metadata descriptor for a database shard partition (§10, §28; US013).
 * Links identity, key boundary range, epoch version, and lifecycle state.
 */
public record ShardMetadata(
        ShardId shardId,
        KeyRange range,
        ShardEpoch epoch,
        ShardLifecycle lifecycle,
        Instant createdAt,
        int replicaCount,
        Map<String, String> attributes) {

    public ShardMetadata {
        Objects.requireNonNull(shardId, "shardId cannot be null");
        Objects.requireNonNull(range, "range cannot be null");
        Objects.requireNonNull(epoch, "epoch cannot be null");
        Objects.requireNonNull(lifecycle, "lifecycle cannot be null");
        Objects.requireNonNull(createdAt, "createdAt cannot be null");
        if (replicaCount < 1) {
            throw new IllegalArgumentException("replicaCount must be positive, got: " + replicaCount);
        }
        attributes = attributes != null ? Collections.unmodifiableMap(new HashMap<>(attributes)) : Collections.emptyMap();
    }

    public static ShardMetadata active(ShardId shardId, KeyRange range, ShardEpoch epoch, int replicaCount) {
        return new ShardMetadata(shardId, range, epoch, ShardLifecycle.ACTIVE, Instant.now(), replicaCount, Collections.emptyMap());
    }

    public static ShardMetadata active(ShardId shardId, int replicaCount) {
        return active(shardId, KeyRange.fullKeyspace(), ShardEpoch.initial(), replicaCount);
    }

    public static ShardMetadata active(ShardId shardId, int replicaCount, Map<String, String> attributes) {
        return new ShardMetadata(shardId, KeyRange.fullKeyspace(), ShardEpoch.initial(), ShardLifecycle.ACTIVE, Instant.now(), replicaCount, attributes);
    }

    public ShardMetadata withRange(KeyRange newRange) {
        return new ShardMetadata(shardId, newRange, epoch, lifecycle, createdAt, replicaCount, attributes);
    }

    public ShardMetadata withEpoch(ShardEpoch newEpoch) {
        return new ShardMetadata(shardId, range, newEpoch, lifecycle, createdAt, replicaCount, attributes);
    }

    public ShardMetadata withLifecycle(ShardLifecycle newLifecycle) {
        return new ShardMetadata(shardId, range, epoch, newLifecycle, createdAt, replicaCount, attributes);
    }
}
