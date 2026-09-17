package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ByteArrayKey;
import se.mouaz.aegisdb.common.ShardId;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Range-based partitioner backed by AtomicReference<TopologySnapshot> (§28, Phase 1 Multi-Raft).
 * Resolves keys to shards in O(log N) time with zero-window atomic cutover.
 */
public class RangePartitioner implements Partitioner {

    private final AtomicReference<TopologySnapshot> currentSnapshot;

    public RangePartitioner(TopologySnapshot initialSnapshot) {
        this.currentSnapshot = new AtomicReference<>(Objects.requireNonNull(initialSnapshot, "initialSnapshot cannot be null"));
    }

    public RangePartitioner(ShardId initialShardId, int replicaCount) {
        this(TopologySnapshot.initialSingleShard(initialShardId, replicaCount));
    }

    @Override
    public ShardId selectShard(String key, ShardMap shardMap) {
        Objects.requireNonNull(key, "key cannot be null");
        return selectShard(ByteArrayKey.of(key));
    }

    /**
     * Resolves the routable shard ID for a key in O(log N) time.
     */
    public ShardId selectShard(ByteArrayKey key) {
        return route(key)
                .map(TopologySnapshot.ShardRangeAssignment::shardId)
                .orElseThrow(() -> new IllegalStateException("No routable shard found for key: " + key));
    }

    /**
     * Resolves the full shard range assignment for a key.
     */
    public Optional<TopologySnapshot.ShardRangeAssignment> route(ByteArrayKey key) {
        return currentSnapshot.get().findRoutableShard(key);
    }

    /**
     * Resolves the authoritative shard assignment (including SPLITTING shards) for a key.
     */
    public Optional<TopologySnapshot.ShardRangeAssignment> routeAuthoritative(ByteArrayKey key) {
        return currentSnapshot.get().findAuthoritativeShard(key);
    }

    /**
     * Atomically replaces the active topology snapshot.
     */
    public void updateSnapshot(TopologySnapshot newSnapshot) {
        Objects.requireNonNull(newSnapshot, "newSnapshot cannot be null");
        this.currentSnapshot.set(newSnapshot);
    }

    /**
     * Atomically swaps the topology snapshot if current matches expected.
     */
    public boolean compareAndSetSnapshot(TopologySnapshot expected, TopologySnapshot update) {
        return this.currentSnapshot.compareAndSet(expected, update);
    }

    public TopologySnapshot currentSnapshot() {
        return currentSnapshot.get();
    }
}
