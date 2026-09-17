package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ByteArrayKey;
import se.mouaz.aegisdb.common.KeyBound;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.ShardLifecycle;

import java.io.Serializable;
import java.util.*;

/**
 * Immutable snapshot of the cluster sharding topology (§28, Phase 1 Multi-Raft).
 * Enforces formal invariants over AuthoritativeShards:
 * 1. SingleAuthoritativeOwner
 * 2. NoAuthoritativeOverlap
 * 3. NoAuthoritativeGaps
 */
public final class TopologySnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    public record ShardRangeAssignment(
            ShardId shardId,
            KeyRange range,
            ShardEpoch epoch,
            ShardLifecycle lifecycle) implements Serializable {
        public ShardRangeAssignment {
            Objects.requireNonNull(shardId, "shardId cannot be null");
            Objects.requireNonNull(range, "range cannot be null");
            Objects.requireNonNull(epoch, "epoch cannot be null");
            Objects.requireNonNull(lifecycle, "lifecycle cannot be null");
        }
    }

    private final TopologyVersion version;
    private final NavigableMap<KeyBound, ShardRangeAssignment> rangeAssignments;
    private final Map<ShardId, ShardMetadata> metadataMap;

    public TopologySnapshot(
            TopologyVersion version,
            Collection<ShardRangeAssignment> assignments,
            Collection<ShardMetadata> metadataCollection) {
        this.version = Objects.requireNonNull(version, "version cannot be null");
        Objects.requireNonNull(assignments, "assignments cannot be null");

        NavigableMap<KeyBound, ShardRangeAssignment> map = new TreeMap<>();
        for (ShardRangeAssignment assignment : assignments) {
            map.put(assignment.range().startBound(), assignment);
        }
        this.rangeAssignments = Collections.unmodifiableNavigableMap(map);

        Map<ShardId, ShardMetadata> metaMap = new HashMap<>();
        if (metadataCollection != null) {
            for (ShardMetadata meta : metadataCollection) {
                metaMap.put(meta.shardId(), meta);
            }
        }
        this.metadataMap = Collections.unmodifiableMap(metaMap);

        // Validate formal invariants upon construction
        validateInvariants();
    }

    /**
     * Creates an initial topology snapshot spanning the full keyspace [-∞, +∞) for a single shard.
     */
    public static TopologySnapshot initialSingleShard(ShardId shardId, int replicaCount) {
        KeyRange full = KeyRange.fullKeyspace();
        ShardEpoch epoch = ShardEpoch.initial();
        ShardRangeAssignment assignment = new ShardRangeAssignment(shardId, full, epoch, ShardLifecycle.ACTIVE);
        ShardMetadata metadata = ShardMetadata.active(shardId, full, epoch, replicaCount);
        return new TopologySnapshot(TopologyVersion.initial(), List.of(assignment), List.of(metadata));
    }

    public TopologyVersion version() {
        return version;
    }

    public NavigableMap<KeyBound, ShardRangeAssignment> rangeAssignments() {
        return rangeAssignments;
    }

    public Map<ShardId, ShardMetadata> metadataMap() {
        return metadataMap;
    }

    /**
     * Resolves the authoritative shard assignment for a key: O(log N) binary search.
     */
    public Optional<ShardRangeAssignment> findAuthoritativeShard(ByteArrayKey key) {
        Objects.requireNonNull(key, "key cannot be null");
        Map.Entry<KeyBound, ShardRangeAssignment> entry = rangeAssignments.floorEntry(KeyBound.exact(key));
        if (entry != null && entry.getValue().range().contains(key) && entry.getValue().lifecycle().isAuthoritative()) {
            return Optional.of(entry.getValue());
        }
        return Optional.empty();
    }

    /**
     * Resolves the routable shard assignment (lifecycle == ACTIVE) for a key: O(log N).
     */
    public Optional<ShardRangeAssignment> findRoutableShard(ByteArrayKey key) {
        Objects.requireNonNull(key, "key cannot be null");
        Map.Entry<KeyBound, ShardRangeAssignment> entry = rangeAssignments.floorEntry(KeyBound.exact(key));
        if (entry != null && entry.getValue().range().contains(key) && entry.getValue().lifecycle().isRoutable()) {
            return Optional.of(entry.getValue());
        }
        return Optional.empty();
    }

    public Optional<ShardMetadata> getMetadata(ShardId shardId) {
        return Optional.ofNullable(metadataMap.get(shardId));
    }

    /**
     * Validates:
     * - NoAuthoritativeOverlap
     * - NoAuthoritativeGaps
     */
    private void validateInvariants() {
        List<ShardRangeAssignment> authoritative = rangeAssignments.values().stream()
                .filter(a -> a.lifecycle().isAuthoritative())
                .sorted(Comparator.comparing(a -> a.range().startBound()))
                .toList();

        if (authoritative.isEmpty()) {
            throw new IllegalStateException("Invariant violation: No authoritative shards in topology snapshot");
        }

        // 1. Verify coverage starts at -∞
        if (!authoritative.get(0).range().startBound().isNegativeInfinity()) {
            throw new IllegalStateException("Invariant violation (NoAuthoritativeGaps): First authoritative range does not start at -∞: " + authoritative.get(0).range());
        }

        // 2. Verify coverage ends at +∞
        if (!authoritative.get(authoritative.size() - 1).range().endBound().isPositiveInfinity()) {
            throw new IllegalStateException("Invariant violation (NoAuthoritativeGaps): Last authoritative range does not end at +∞: " + authoritative.get(authoritative.size() - 1).range());
        }

        // 3. Verify contiguous adjacency without gaps or overlaps
        for (int i = 0; i < authoritative.size() - 1; i++) {
            ShardRangeAssignment current = authoritative.get(i);
            ShardRangeAssignment next = authoritative.get(i + 1);

            if (current.range().overlaps(next.range())) {
                throw new IllegalStateException("Invariant violation (NoAuthoritativeOverlap): Overlapping ranges " + current.range() + " and " + next.range());
            }

            if (!current.range().endBound().equals(next.range().startBound())) {
                throw new IllegalStateException("Invariant violation (NoAuthoritativeGaps): Gap between " + current.range() + " and " + next.range());
            }
        }
    }
}
