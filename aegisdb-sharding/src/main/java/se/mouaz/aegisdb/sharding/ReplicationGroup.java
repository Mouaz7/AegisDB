package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents the Raft consensus replication group for a database shard (Master Project Plan §5, §10; US013).
 * Encapsulates the ShardId and the set of replica nodes participating in consensus for that shard.
 */
public record ReplicationGroup(ShardId shardId, Set<NodeId> members) {

    public ReplicationGroup {
        Objects.requireNonNull(shardId, "shardId cannot be null");
        Objects.requireNonNull(members, "members cannot be null");
        if (members.isEmpty()) {
            throw new IllegalArgumentException("ReplicationGroup members cannot be empty for shard: " + shardId);
        }
        members = Collections.unmodifiableSet(new LinkedHashSet<>(members));
    }

    public static ReplicationGroup of(ShardId shardId, Set<NodeId> members) {
        return new ReplicationGroup(shardId, members);
    }

    public static ReplicationGroup of(ShardId shardId, NodeId... members) {
        Objects.requireNonNull(members, "members cannot be null");
        Set<NodeId> set = new LinkedHashSet<>();
        Collections.addAll(set, members);
        return new ReplicationGroup(shardId, set);
    }

    public boolean contains(NodeId nodeId) {
        return members.contains(nodeId);
    }

    public int size() {
        return members.size();
    }
}
