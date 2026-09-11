package se.mouaz.aegisdb.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.NodeConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ClusterId;
import se.mouaz.aegisdb.common.Endpoint;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Standalone launcher for AegisDB nodes (Master Project Plan §11).
 * Allows starting a single node from the command line.
 */
public class StandaloneLauncher {
    private static final Logger log = LoggerFactory.getLogger(StandaloneLauncher.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: java StandaloneLauncher <nodeId> <port> <dataDir> [peerId:peerHost:peerPort...]");
            System.exit(1);
        }

        String nodeIdStr = args[0];
        int port = Integer.parseInt(args[1]);
        String dataDir = args[2];

        NodeId nodeId = NodeId.of(nodeIdStr);
        NodeConfiguration nodeConfig = NodeConfiguration.builder()
                .nodeId(nodeId)
                .endpoint(Endpoint.of("localhost", port))
                .dataDir(Path.of(dataDir))
                .electionTimeout(Duration.ofMillis(300))
                .heartbeatInterval(Duration.ofMillis(100))
                .build();

        ClusterConfiguration.Builder clusterBuilder = ClusterConfiguration.builder()
                .clusterId(ClusterId.of("aegisdb-cluster"));
        
        for (int i = 3; i < args.length; i++) {
            String[] parts = args[i].split(":");
            if (parts.length == 3) {
                clusterBuilder.addMember(parts[0], parts[1], Integer.parseInt(parts[2]));
            }
        }
        ClusterConfiguration clusterConfig = clusterBuilder.build();

        log.info("Bootstrapping AegisDB Node: {}", nodeId);
        
        try {
            DatabaseNode node = NodeBootstrap.createGrpcNode(nodeConfig, clusterConfig);
            node.start();
            
            // Add shutdown hook to cleanly stop the node on SIGINT
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received, stopping node...");
                node.stop();
                log.info("Node stopped.");
            }));
            
            log.info("Node {} successfully started. Press Ctrl+C to stop.", nodeId);
            
            // Block main thread to keep JVM alive
            Thread.currentThread().join();
        } catch (Exception e) {
            log.error("Failed to start AegisDB node", e);
            System.exit(1);
        }
    }
}
