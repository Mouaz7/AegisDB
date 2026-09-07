package se.mouaz.jdistdb.common;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

public record NodeConfiguration(
    NodeId nodeId,
    Endpoint endpoint,
    Path dataDir,
    Duration electionTimeout,
    Duration heartbeatInterval,
    NetworkConfiguration networkConfig
) {
    public NodeConfiguration {
        Objects.requireNonNull(nodeId, "nodeId cannot be null");
        Objects.requireNonNull(endpoint, "endpoint cannot be null");
        if (dataDir == null) {
            dataDir = Path.of("data", nodeId.value());
        }
        if (electionTimeout == null) {
            electionTimeout = Duration.ofMillis(300);
        }
        if (heartbeatInterval == null) {
            heartbeatInterval = Duration.ofMillis(100);
        }
        if (networkConfig == null) {
            networkConfig = NetworkConfiguration.defaultConfiguration();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private NodeId nodeId;
        private Endpoint endpoint;
        private Path dataDir;
        private Duration electionTimeout = Duration.ofMillis(300);
        private Duration heartbeatInterval = Duration.ofMillis(100);
        private NetworkConfiguration networkConfig = NetworkConfiguration.defaultConfiguration();

        public Builder nodeId(NodeId nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder nodeId(String nodeId) {
            this.nodeId = NodeId.of(nodeId);
            return this;
        }

        public Builder endpoint(Endpoint endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder endpoint(String host, int port) {
            this.endpoint = Endpoint.of(host, port);
            return this;
        }

        public Builder dataDir(Path dataDir) {
            this.dataDir = dataDir;
            return this;
        }

        public Builder electionTimeout(Duration electionTimeout) {
            this.electionTimeout = electionTimeout;
            return this;
        }

        public Builder heartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder networkConfig(NetworkConfiguration networkConfig) {
            this.networkConfig = networkConfig;
            return this;
        }

        public NodeConfiguration build() {
            return new NodeConfiguration(nodeId, endpoint, dataDir, electionTimeout, heartbeatInterval, networkConfig);
        }
    }
}
