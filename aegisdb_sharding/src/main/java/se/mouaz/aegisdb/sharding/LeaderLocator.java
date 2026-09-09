package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;

import java.util.Map;
import java.util.Optional;

/**
 * Interface for tracking and caching the active Raft leader for each shard (Master Project Plan §10; US014).
 */
public interface LeaderLocator {

    /**
     * Gets the currently known leader node for a shard.
     */
    Optional<NodeId> getLeader(ShardId shardId);

    /**
     * Updates or caches the known leader node for a shard.
     */
    void updateLeader(ShardId shardId, NodeId leaderId);

    /**
     * Invalidates the cached leader for a shard if it matches the suspected stale leader.
     */
    void invalidateLeader(ShardId shardId, NodeId staleLeaderId);

    /**
     * Unconditionally invalidates the cached leader for a shard.
     */
    void invalidateLeader(ShardId shardId);

    /**
     * Returns an immutable snapshot map of all currently tracked shard leaders.
     */
    Map<ShardId, NodeId> allLeaders();

    /**
     * Listener callback invoked upon shard leader changes.
     */
    @FunctionalInterface
    interface LeaderChangeListener {
        void onLeaderChanged(ShardId shardId, Optional<NodeId> oldLeader, Optional<NodeId> newLeader);
    }

    /**
     * Registers a listener for leader change notifications.
     */
    void addListener(LeaderChangeListener listener);
}
