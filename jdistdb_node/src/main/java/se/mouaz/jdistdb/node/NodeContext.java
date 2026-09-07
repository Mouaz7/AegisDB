package se.mouaz.jdistdb.node;

import se.mouaz.jdistdb.common.ClusterConfiguration;
import se.mouaz.jdistdb.common.NodeConfiguration;
import se.mouaz.jdistdb.common.NodeId;
import se.mouaz.jdistdb.transport.RaftTransport;

import java.util.Objects;

public record NodeContext(
    NodeConfiguration config,
    ClusterConfiguration clusterConfig,
    RaftTransport transport
) {
    public NodeContext {
        Objects.requireNonNull(config, "config cannot be null");
        Objects.requireNonNull(clusterConfig, "clusterConfig cannot be null");
        Objects.requireNonNull(transport, "transport cannot be null");
    }

    public NodeId nodeId() {
        return config.nodeId();
    }
}
