package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ShardId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Thread-safe registry and topology map of active shards and their replication groups (Master Project Plan §5, §10; US013).
 * Supports deterministic indexed lookups, id lookups, and dynamic membership updates.
 */
public class ShardMap {

    private final Map<ShardId, Shard> shardRegistry = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Shard> indexedShards = new CopyOnWriteArrayList<>();

    public ShardMap() {
    }

    public ShardMap(Collection<Shard> initialShards) {
        if (initialShards != null) {
            for (Shard shard : initialShards) {
                registerShard(shard);
            }
        }
    }

    /**
     * Registers or updates a shard in the topology map.
     */
    public synchronized void registerShard(Shard shard) {
        Objects.requireNonNull(shard, "shard cannot be null");
        Shard previous = shardRegistry.put(shard.id(), shard);
        if (previous != null) {
            indexedShards.remove(previous);
        }
        indexedShards.add(shard);
        // Maintain deterministic ordering by ShardId
        indexedShards.sort(Comparator.comparing(Shard::id));
    }

    /**
     * Removes a shard from the topology map.
     */
    public synchronized Optional<Shard> removeShard(ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId cannot be null");
        Shard removed = shardRegistry.remove(shardId);
        if (removed != null) {
            indexedShards.remove(removed);
        }
        return Optional.ofNullable(removed);
    }

    /**
     * Retrieves a shard by its unique ShardId.
     */
    public Optional<Shard> getShard(ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId cannot be null");
        return Optional.ofNullable(shardRegistry.get(shardId));
    }

    /**
     * Retrieves a shard by its 0-based topological index.
     */
    public Optional<Shard> getShardByIndex(int index) {
        if (index < 0 || index >= indexedShards.size()) {
            return Optional.empty();
        }
        return Optional.of(indexedShards.get(index));
    }

    /**
     * Checks if the topology contains the given shard.
     */
    public boolean containsShard(ShardId shardId) {
        return shardRegistry.containsKey(shardId);
    }

    /**
     * Returns the total number of registered shards.
     */
    public int shardCount() {
        return indexedShards.size();
    }

    /**
     * Returns an immutable snapshot list of all active shards, sorted by ShardId.
     */
    public List<Shard> allShards() {
        return Collections.unmodifiableList(new ArrayList<>(indexedShards));
    }
}
