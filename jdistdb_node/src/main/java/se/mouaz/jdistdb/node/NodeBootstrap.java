package se.mouaz.jdistdb.node;

import se.mouaz.jdistdb.common.ClusterConfiguration;
import se.mouaz.jdistdb.common.NodeConfiguration;
import se.mouaz.jdistdb.transport.GrpcRaftTransport;
import se.mouaz.jdistdb.transport.InMemoryTransport;
import se.mouaz.jdistdb.transport.RaftTransport;

public final class NodeBootstrap {

    private NodeBootstrap() {}

    public static DatabaseNode createInMemoryNode(NodeConfiguration nodeConfig, ClusterConfiguration clusterConfig) {
        RaftTransport transport = new InMemoryTransport(nodeConfig.nodeId(), nodeConfig.networkConfig().requestTimeout());
        return new DatabaseNode(nodeConfig, clusterConfig, transport);
    }

    public static DatabaseNode createGrpcNode(NodeConfiguration nodeConfig, ClusterConfiguration clusterConfig) {
        RaftTransport transport = new GrpcRaftTransport(
                nodeConfig.nodeId(),
                nodeConfig.endpoint(),
                clusterConfig,
                nodeConfig.networkConfig().requestTimeout()
        );
        return new DatabaseNode(nodeConfig, clusterConfig, transport);
    }
}
