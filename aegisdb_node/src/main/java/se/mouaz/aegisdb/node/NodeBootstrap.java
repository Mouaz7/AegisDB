package se.mouaz.aegisdb.node;

import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.NodeConfiguration;
import se.mouaz.aegisdb.transport.GrpcRaftTransport;
import se.mouaz.aegisdb.transport.InMemoryTransport;
import se.mouaz.aegisdb.transport.RaftTransport;

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

    public static DatabaseNode createPersistentNode(NodeConfiguration nodeConfig,
                                                    ClusterConfiguration clusterConfig,
                                                    se.mouaz.aegisdb.storage.StorageEngine storageEngine,
                                                    se.mouaz.aegisdb.raft.RaftNode raftNode) {
        RaftTransport transport = new InMemoryTransport(nodeConfig.nodeId(), nodeConfig.networkConfig().requestTimeout());
        return new DatabaseNode(nodeConfig, clusterConfig, transport, storageEngine, raftNode);
    }
}
