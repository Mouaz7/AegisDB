package se.mouaz.aegisdb.node;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.NodeConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ClusterId;
import se.mouaz.aegisdb.common.Endpoint;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Standalone launcher for AegisDB nodes (Master Project Plan §11).
 * Allows starting a single node from the command line.
 */
public class StandaloneLauncher {
    private static final Logger log = LoggerFactory.getLogger(StandaloneLauncher.class);

    public static void main(String[] args) {
        String configFile = null;
        String nodeIdStr = null;

        for (int i = 0; i < args.length; i++) {
            if ("--config".equals(args[i]) && i + 1 < args.length) {
                configFile = args[++i];
            } else if ("--node-id".equals(args[i]) && i + 1 < args.length) {
                nodeIdStr = args[++i];
            } else {
                System.err.println("Unknown parameter passed: " + args[i]);
                System.exit(1);
            }
        }

        if (configFile == null || nodeIdStr == null) {
            System.err.println("Usage: java se.mouaz.aegisdb.node.StandaloneLauncher --config <path> --node-id <id>");
            System.exit(1);
        }

        File configPath = new File(configFile);
        if (!configPath.exists() || !configPath.isFile()) {
            System.err.println("Configuration file not found: " + configFile);
            System.exit(1);
        }

        JsonNode root;
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            root = mapper.readTree(configPath);
        } catch (Exception e) {
            System.err.println("Failed to parse YAML configuration: " + e.getMessage());
            System.exit(1);
            return;
        }

        JsonNode clusterNode = root.path("cluster");
        String clusterIdStr = clusterNode.path("clusterId").asText(null);
        if (clusterIdStr == null) {
            System.err.println("Missing cluster.clusterId in configuration");
            System.exit(1);
        }

        JsonNode nodesArray = clusterNode.path("nodes");
        if (!nodesArray.isArray() || nodesArray.isEmpty()) {
            System.err.println("Missing or empty cluster.nodes array in configuration");
            System.exit(1);
        }

        JsonNode storageNode = root.path("storage");
        String dataDirBase = storageNode.path("dataDir").asText(null);
        if (dataDirBase == null) {
            System.err.println("Missing storage.dataDir in configuration");
            System.exit(1);
        }

        JsonNode raftNode = root.path("raft");
        long electionTimeoutMaxMs = raftNode.path("electionTimeoutMaxMs").asLong(300);
        long heartbeatIntervalMs = raftNode.path("heartbeatIntervalMs").asLong(100);

        ClusterConfiguration.Builder clusterBuilder = ClusterConfiguration.builder()
                .clusterId(ClusterId.of(clusterIdStr));

        JsonNode myNodeConfig = null;

        for (JsonNode nodeItem : nodesArray) {
            String nId = nodeItem.path("id").asText(null);
            String nHost = nodeItem.path("host").asText(null);
            int nPort = nodeItem.path("port").asInt(-1);

            if (nId == null || nHost == null || nPort <= 0 || nPort > 65535) {
                System.err.println("Invalid node configuration, missing/invalid id, host, or port: " + nodeItem);
                System.exit(1);
            }

            clusterBuilder.addMember(nId, nHost, nPort);

            if (nId.equals(nodeIdStr)) {
                myNodeConfig = nodeItem;
            }
        }

        if (myNodeConfig == null) {
            System.err.println("Unknown node ID: " + nodeIdStr + " (not found in configuration)");
            System.exit(1);
        }

        NodeId nodeId = NodeId.of(nodeIdStr);
        String myHost = myNodeConfig.path("host").asText();
        int myPort = myNodeConfig.path("port").asInt();
        Path nodeDataDir = Path.of(dataDirBase, nodeIdStr);

        NodeConfiguration nodeConfig = NodeConfiguration.builder()
                .nodeId(nodeId)
                .endpoint(Endpoint.of(myHost, myPort))
                .dataDir(nodeDataDir)
                .electionTimeout(Duration.ofMillis(electionTimeoutMaxMs))
                .heartbeatInterval(Duration.ofMillis(heartbeatIntervalMs))
                .build();

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
