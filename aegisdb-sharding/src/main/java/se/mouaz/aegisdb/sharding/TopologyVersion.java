package se.mouaz.aegisdb.sharding;

import java.io.Serializable;
import java.util.Objects;

/**
 * Cluster-wide monotonically increasing topology revision counter (§28, Phase 1 Multi-Raft).
 * Published by the Metadata Raft Group upon committed topology cutovers.
 */
public record TopologyVersion(long version) implements Comparable<TopologyVersion>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final TopologyVersion INITIAL = new TopologyVersion(1L);

    public TopologyVersion {
        if (version < 0) {
            throw new IllegalArgumentException("TopologyVersion must be non-negative, got: " + version);
        }
    }

    public static TopologyVersion initial() {
        return INITIAL;
    }

    public static TopologyVersion of(long version) {
        return new TopologyVersion(version);
    }

    public TopologyVersion next() {
        return new TopologyVersion(this.version + 1);
    }

    @Override
    public int compareTo(TopologyVersion other) {
        Objects.requireNonNull(other, "other cannot be null");
        return Long.compare(this.version, other.version);
    }

    @Override
    public String toString() {
        return "TopologyVersion(" + version + ")";
    }
}
