package se.mouaz.aegisdb.sharding;

import java.io.Serializable;
import java.util.Objects;

/**
 * Versioning vector for a shard partition (§28, Phase 1 Multi-Raft).
 * - generation increments on shard membership changes (replica rebalancing).
 * - version increments on range topology updates (splits/merges).
 */
public record ShardEpoch(long generation, long version) implements Comparable<ShardEpoch>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final ShardEpoch INITIAL = new ShardEpoch(1L, 0L);

    public ShardEpoch {
        if (generation < 0 || version < 0) {
            throw new IllegalArgumentException("Epoch components must be non-negative, got: " + generation + ", " + version);
        }
    }

    public static ShardEpoch initial() {
        return INITIAL;
    }

    public static ShardEpoch of(long generation, long version) {
        return new ShardEpoch(generation, version);
    }

    public ShardEpoch nextVersion() {
        return new ShardEpoch(this.generation, this.version + 1);
    }

    public ShardEpoch nextGeneration() {
        return new ShardEpoch(this.generation + 1, 0L);
    }

    @Override
    public int compareTo(ShardEpoch other) {
        Objects.requireNonNull(other, "other cannot be null");
        int cmp = Long.compare(this.generation, other.generation);
        if (cmp != 0) {
            return cmp;
        }
        return Long.compare(this.version, other.version);
    }

    @Override
    public String toString() {
        return "ShardEpoch(" + generation + "." + version + ")";
    }
}
