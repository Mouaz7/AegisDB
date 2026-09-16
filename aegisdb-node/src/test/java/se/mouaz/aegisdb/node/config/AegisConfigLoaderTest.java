package se.mouaz.aegisdb.node.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AegisConfigLoaderTest {

    private String resolveConfigFile(String relativePath) {
        Path p = Path.of(relativePath);
        if (Files.exists(p)) return p.toString();
        Path parentP = Path.of("..", relativePath);
        if (Files.exists(parentP)) return parentP.toString();
        return relativePath;
    }

    @Test
    @DisplayName("AegisConfigLoader loads canonical example configuration successfully")
    void testLoadCanonicalExampleConfig() throws Exception {
        String configFile = resolveConfigFile("config/aegisdb-cluster.example.yaml");
        AegisConfig config = AegisConfigLoader.load(configFile, "node-1");

        assertThat(config).isNotNull();
        assertThat(config.nodeConfig().nodeId().value()).isEqualTo("node-1");
        assertThat(config.nodeConfig().endpoint().port()).isEqualTo(7001);
        assertThat(config.clusterConfig().members()).hasSize(3);
        assertThat(config.adminToken()).isEqualTo("CHANGE_ME_LOCAL_DEV_TOKEN");
        assertThat(config.tlsEnabled()).isFalse();
        assertThat(config.managementEnabled()).isTrue();
        assertThat(config.managementPort()).isEqualTo(9001);
    }

    @Test
    @DisplayName("AegisConfigLoader throws when requested node-id does not exist")
    void testNodeNotFoundThrowsException() {
        String configFile = resolveConfigFile("config/aegisdb-cluster.example.yaml");
        assertThatThrownBy(() -> AegisConfigLoader.load(configFile, "unknown-node"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown node ID: unknown-node");
    }

    @Test
    @DisplayName("AegisConfigLoader detects duplicate node IDs and fails closed")
    void testDuplicateNodeIdDetection(@TempDir Path tempDir) throws Exception {
        String yaml = """
                cluster:
                  cluster_id: "test-cluster"
                  shards: 1
                  replication_factor: 1
                storage:
                  data_dir: "target/data"
                nodes:
                  - node_id: "node-1"
                    endpoint:
                      host: "127.0.0.1"
                      port: 7001
                    raft_port: 8001
                  - node_id: "node-1"
                    endpoint:
                      host: "127.0.0.1"
                      port: 7002
                    raft_port: 8002
                """;
        Path configFile = tempDir.resolve("duplicate-nodes.yaml");
        Files.writeString(configFile, yaml);

        assertThatThrownBy(() -> AegisConfigLoader.load(configFile.toString(), "node-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate node ID detected in configuration: node-1");
    }

    @Test
    @DisplayName("AegisConfigLoader fails closed when TLS is enabled without certificate paths")
    void testTlsFailClosedValidation(@TempDir Path tempDir) throws Exception {
        String yaml = """
                cluster:
                  cluster_id: "test-cluster"
                  shards: 1
                  replication_factor: 1
                storage:
                  data_dir: "target/data"
                nodes:
                  - node_id: "node-1"
                    endpoint:
                      host: "127.0.0.1"
                      port: 7001
                    raft_port: 8001
                security:
                  tls:
                    enabled: true
                    cert_path: ""
                    key_path: ""
                """;
        Path configFile = tempDir.resolve("invalid-tls.yaml");
        Files.writeString(configFile, yaml);

        assertThatThrownBy(() -> AegisConfigLoader.load(configFile.toString(), "node-1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TLS is enabled but cert_path or key_path is missing or empty");
    }
}
