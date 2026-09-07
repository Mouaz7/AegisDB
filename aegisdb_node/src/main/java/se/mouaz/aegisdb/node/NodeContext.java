package se.mouaz.aegisdb.node;

import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.NodeConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.transport.RaftTransport;

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
