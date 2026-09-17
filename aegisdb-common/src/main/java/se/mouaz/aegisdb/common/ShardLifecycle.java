package se.mouaz.aegisdb.common;

/**
 * Deterministic shard lifecycle states (§28, Phase 1 Multi-Raft).
 */
public enum ShardLifecycle {
    ACTIVE,
    SPLITTING,
    BOOTSTRAPPING,
    READY,
    RETIRED_RANGE,
    TOMBSTONED;

    public boolean isAuthoritative() {
        return this == ACTIVE || this == SPLITTING;
    }

    public boolean isRoutable() {
        return this == ACTIVE;
    }
}
