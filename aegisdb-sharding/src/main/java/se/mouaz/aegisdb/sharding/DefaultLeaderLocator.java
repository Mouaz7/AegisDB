package se.mouaz.aegisdb.sharding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Standard thread-safe implementation of LeaderLocator (Master Project Plan §10; US014).
 * Caches active leaders per shard, invalidates stale entries upon redirect/failure,
 * and notifies listeners of leader transitions.
 */
public class DefaultLeaderLocator implements LeaderLocator {
    private static final Logger log = LoggerFactory.getLogger(DefaultLeaderLocator.class);

    private final Map<ShardId, NodeId> leaderCache = new ConcurrentHashMap<>();
    private final List<LeaderChangeListener> listeners = new CopyOnWriteArrayList<>();

    public DefaultLeaderLocator() {
    }

    public DefaultLeaderLocator(Map<ShardId, NodeId> initialLeaders) {
        if (initialLeaders != null) {
            leaderCache.putAll(initialLeaders);
        }
    }

    @Override
    public Optional<NodeId> getLeader(ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId cannot be null");
        return Optional.ofNullable(leaderCache.get(shardId));
    }

    @Override
    public void updateLeader(ShardId shardId, NodeId newLeader) {
        Objects.requireNonNull(shardId, "shardId cannot be null");
        Objects.requireNonNull(newLeader, "newLeader cannot be null");

        NodeId previous = leaderCache.put(shardId, newLeader);
        if (!Objects.equals(previous, newLeader)) {
            log.info("Leader for shard {} updated from {} to {}", shardId, previous, newLeader);
            notifyListeners(shardId, Optional.ofNullable(previous), Optional.of(newLeader));
        }
    }

    @Override
    public void invalidateLeader(ShardId shardId, NodeId staleLeaderId) {
        Objects.requireNonNull(shardId, "shardId cannot be null");
        if (staleLeaderId == null) {
            invalidateLeader(shardId);
            return;
        }

        boolean removed = leaderCache.remove(shardId, staleLeaderId);
        if (removed) {
            log.info("Invalidated stale leader {} for shard {}", staleLeaderId, shardId);
            notifyListeners(shardId, Optional.of(staleLeaderId), Optional.empty());
        }
    }

    @Override
    public void invalidateLeader(ShardId shardId) {
        Objects.requireNonNull(shardId, "shardId cannot be null");
        NodeId removed = leaderCache.remove(shardId);
        if (removed != null) {
            log.info("Invalidated cached leader {} for shard {}", removed, shardId);
            notifyListeners(shardId, Optional.of(removed), Optional.empty());
        }
    }

    @Override
    public Map<ShardId, NodeId> allLeaders() {
        return Collections.unmodifiableMap(new HashMap<>(leaderCache));
    }

    @Override
    public void addListener(LeaderChangeListener listener) {
        Objects.requireNonNull(listener, "listener cannot be null");
        listeners.add(listener);
    }

    private void notifyListeners(ShardId shardId, Optional<NodeId> oldLeader, Optional<NodeId> newLeader) {
        for (LeaderChangeListener listener : listeners) {
            try {
                listener.onLeaderChanged(shardId, oldLeader, newLeader);
            } catch (Exception e) {
                log.warn("Error notifying LeaderChangeListener for shard {}", shardId, e);
            }
        }
    }
}
