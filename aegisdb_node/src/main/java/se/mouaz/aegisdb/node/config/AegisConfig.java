package se.mouaz.aegisdb.node.config;

import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.NodeConfiguration;

public record AegisConfig(
    ClusterConfiguration clusterConfig,
    NodeConfiguration nodeConfig,
    String adminToken,
    String monitorToken,
    boolean tlsEnabled,
    String tlsCertPath,
    String tlsKeyPath,
    boolean managementEnabled,
    int managementPort
) {}
