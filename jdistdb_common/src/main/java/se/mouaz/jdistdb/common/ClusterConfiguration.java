package se.mouaz.jdistdb.common;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public record ClusterConfiguration(
    ClusterId clusterId,
    Map<NodeId, Endpoint> members
) {
    public ClusterConfiguration {
        Objects.requireNonNull(clusterId, "clusterId cannot be null");
        members = members == null ? Map.of() : Map.copyOf(members);
    }

    public Optional<Endpoint> getEndpoint(NodeId nodeId) {
        return Optional.ofNullable(members.get(nodeId));
    }

    public boolean contains(NodeId nodeId) {
        return members.containsKey(nodeId);
    }

    public Set<NodeId> memberIds() {
        return members.keySet();
    }

    public int clusterSize() {
        return members.size();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ClusterId clusterId = ClusterId.of("jdistdb-cluster");
        private final Map<NodeId, Endpoint> members = new HashMap<>();

        public Builder clusterId(ClusterId clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder clusterId(String clusterId) {
            this.clusterId = ClusterId.of(clusterId);
            return this;
        }

        public Builder addMember(NodeId nodeId, Endpoint endpoint) {
            this.members.put(nodeId, endpoint);
            return this;
        }

        public Builder addMember(String nodeId, String host, int port) {
            this.members.put(NodeId.of(nodeId), Endpoint.of(host, port));
            return this;
        }

        public ClusterConfiguration build() {
            return new ClusterConfiguration(clusterId, members);
        }
    }
}
