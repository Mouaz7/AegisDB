package se.mouaz.aegisdb.node.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.ClusterId;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeConfiguration;
import se.mouaz.aegisdb.common.NodeId;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;

public class AegisConfigLoader {

    public static AegisConfig load(String configFile, String targetNodeId) throws Exception {
        File configPath = new File(configFile);
        if (!configPath.exists() || !configPath.isFile()) {
            throw new IllegalArgumentException("Configuration file not found: " + configFile);
        }

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        JsonNode root = mapper.readTree(configPath);

        JsonNode clusterNode = root.path("cluster");
        String clusterIdStr = clusterNode.path("clusterId").asText(clusterNode.path("cluster_id").asText(null));
        if (clusterIdStr == null) {
            throw new IllegalArgumentException("Missing cluster.cluster_id in configuration");
        }

        JsonNode nodesArray = root.path("nodes");
        if (!nodesArray.isArray() || nodesArray.isEmpty()) {
            throw new IllegalArgumentException("Missing or empty nodes array in configuration");
        }

        JsonNode storageNode = root.path("storage");
        String dataDirBase = storageNode.path("dataDir").asText(storageNode.path("data_dir").asText(null));
        if (dataDirBase == null) {
            throw new IllegalArgumentException("Missing storage.data_dir in configuration");
        }

        JsonNode raftNode = root.path("raft");
        long electionTimeoutMaxMs = raftNode.path("electionTimeoutMaxMs").asLong(raftNode.path("election_timeout_max_ms").asLong(300));
        long heartbeatIntervalMs = raftNode.path("heartbeatIntervalMs").asLong(raftNode.path("heartbeat_interval_ms").asLong(100));

        ClusterConfiguration.Builder clusterBuilder = ClusterConfiguration.builder()
                .clusterId(ClusterId.of(clusterIdStr));

        JsonNode myNodeConfig = null;
        java.util.Set<String> seenNodeIds = new java.util.HashSet<>();
        java.util.Set<String> seenEndpoints = new java.util.HashSet<>();

        for (JsonNode nodeItem : nodesArray) {
            String nId = nodeItem.path("id").asText(nodeItem.path("node_id").asText(null));
            JsonNode endpointNode = nodeItem.path("endpoint");
            String nHost = endpointNode.path("host").asText(null);
            int nPort = endpointNode.path("port").asInt(-1);

            if (nId == null || nHost == null || nPort <= 0 || nPort > 65535) {
                throw new IllegalArgumentException("Invalid node configuration, missing/invalid id, host, or port: " + nodeItem);
            }

            if (!seenNodeIds.add(nId)) {
                throw new IllegalArgumentException("Duplicate node ID detected in configuration: " + nId);
            }

            if (!seenEndpoints.add(nHost + ":" + nPort)) {
                throw new IllegalArgumentException("Duplicate node endpoint detected in configuration: " + nHost + ":" + nPort);
            }

            clusterBuilder.addMember(nId, nHost, nPort);

            if (nId.equals(targetNodeId)) {
                myNodeConfig = nodeItem;
            }
        }

        if (myNodeConfig == null) {
            throw new IllegalArgumentException("Unknown node ID: " + targetNodeId + " (not found in configuration)");
        }

        NodeId nodeId = NodeId.of(targetNodeId);
        JsonNode endpointNode = myNodeConfig.path("endpoint");
        String myHost = endpointNode.path("host").asText();
        int myPort = endpointNode.path("port").asInt();
        Path nodeDataDir = Path.of(dataDirBase, targetNodeId);

        NodeConfiguration nodeConfig = NodeConfiguration.builder()
                .nodeId(nodeId)
                .endpoint(Endpoint.of(myHost, myPort))
                .dataDir(nodeDataDir)
                .electionTimeout(Duration.ofMillis(electionTimeoutMaxMs))
                .heartbeatInterval(Duration.ofMillis(heartbeatIntervalMs))
                .build();

        ClusterConfiguration clusterConfig = clusterBuilder.build();

        JsonNode managementNode = myNodeConfig.path("management");
        boolean managementEnabled = managementNode.path("enabled").asBoolean(false);
        int managementPort = managementNode.path("port").asInt(9001);
        if (managementEnabled && (managementPort <= 0 || managementPort > 65535)) {
            throw new IllegalArgumentException("Invalid management port: " + managementPort);
        }

        JsonNode securityNode = root.path("security");
        JsonNode rbacNode = securityNode.path("rbac");
        String adminToken = resolveEnv(rbacNode.path("admin_token").asText(""));
        String monitorToken = resolveEnv(rbacNode.path("monitor_token").asText(""));

        JsonNode tlsNode = securityNode.path("tls");
        boolean tlsEnabled = tlsNode.path("enabled").asBoolean(false);
        String tlsCertPath = resolveEnv(tlsNode.path("cert_path").asText(""));
        String tlsKeyPath = resolveEnv(tlsNode.path("key_path").asText(""));

        // Fail closed validation
        if (tlsEnabled && (tlsCertPath.isEmpty() || tlsKeyPath.isEmpty())) {
            throw new IllegalArgumentException("TLS is enabled but cert_path or key_path is missing or empty.");
        }

        return new AegisConfig(
                clusterConfig,
                nodeConfig,
                adminToken,
                monitorToken,
                tlsEnabled,
                tlsCertPath,
                tlsKeyPath,
                managementEnabled,
                managementPort
        );
    }

    private static String resolveEnv(String value) {
        if (value == null) return "";
        if (value.startsWith("${") && value.endsWith("}")) {
            String envVar = value.substring(2, value.length() - 1);
            String envVal = System.getenv(envVar);
            return envVal != null ? envVal : "";
        }
        return value;
    }
}
