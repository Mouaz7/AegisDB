package se.mouaz.aegisdb.sharding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;

import java.util.*;

/**
 * Coordinates shard topology, replication group configurations, and shard lifecycle management (Master Project Plan §10; US013).
 */
public class ShardManager {
    private static final Logger log = LoggerFactory.getLogger(ShardManager.class);

    private final ShardMap shardMap;
    private final LeaderLocator leaderLocator;
    private final ShardRouter router;

    public ShardManager(ShardMap shardMap, LeaderLocator leaderLocator, Partitioner partitioner) {
        this.shardMap = Objects.requireNonNull(shardMap, "shardMap cannot be null");
        this.leaderLocator = Objects.requireNonNull(leaderLocator, "leaderLocator cannot be null");
        this.router = new ShardRouter(Objects.requireNonNull(partitioner, "partitioner cannot be null"), this.shardMap);
    }

    public ShardManager(ShardMap shardMap, LeaderLocator leaderLocator) {
        this(shardMap, leaderLocator, new HashPartitioner());
    }

    public ShardManager() {
        this(new ShardMap(), new DefaultLeaderLocator(), new HashPartitioner());
    }

    /**
     * Bootstraps static shards distributed evenly across cluster nodes.
     *
     * @param shardCount        number of shards to create
     * @param clusterNodes      available cluster nodes
     * @param replicationFactor number of replica nodes per shard
     * @return initialized ShardManager instance
     */
    public static ShardManager createStaticShards(int shardCount, List<NodeId> clusterNodes, int replicationFactor) {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be positive");
        }
        if (clusterNodes == null || clusterNodes.isEmpty()) {
            throw new IllegalArgumentException("clusterNodes cannot be empty");
        }
        if (replicationFactor <= 0 || replicationFactor > clusterNodes.size()) {
            throw new IllegalArgumentException("Invalid replicationFactor: " + replicationFactor
                    + " for node count " + clusterNodes.size());
        }

        ShardMap shardMap = new ShardMap();
        LeaderLocator leaderLocator = new DefaultLeaderLocator();

        for (int i = 0; i < shardCount; i++) {
            ShardId shardId = ShardId.of(i);
            Set<NodeId> replicas = new LinkedHashSet<>();
            for (int r = 0; r < replicationFactor; r++) {
                int nodeIndex = (i + r) % clusterNodes.size();
                replicas.add(clusterNodes.get(nodeIndex));
            }
            ReplicationGroup group = ReplicationGroup.of(shardId, replicas);
            Shard shard = Shard.of(shardId, group);
            shardMap.registerShard(shard);

            // Default initial leader assumption to first replica
            leaderLocator.updateLeader(shardId, replicas.iterator().next());
            log.info("Initialized static shard {} with replicas {}", shardId, replicas);
        }

        return new ShardManager(shardMap, leaderLocator);
    }

    public void registerShard(Shard shard) {
        shardMap.registerShard(shard);
    }

    public void updateLeader(ShardId shardId, NodeId leaderId) {
        leaderLocator.updateLeader(shardId, leaderId);
    }

    public ShardMap shardMap() {
        return shardMap;
    }

    public LeaderLocator leaderLocator() {
        return leaderLocator;
    }

    public ShardRouter router() {
        return router;
    }
}
