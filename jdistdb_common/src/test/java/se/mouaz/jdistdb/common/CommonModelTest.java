package se.mouaz.jdistdb.common;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CommonModelTest {

    @Test
    void testNodeIdValidation() {
        NodeId id = NodeId.of("node-1");
        assertThat(id.value()).isEqualTo("node-1");
        assertThat(id.toString()).isEqualTo("node-1");

        assertThatThrownBy(() -> new NodeId(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new NodeId("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEndpointValidationAndParsing() {
        Endpoint ep = Endpoint.of("localhost", 7001);
        assertThat(ep.host()).isEqualTo("localhost");
        assertThat(ep.port()).isEqualTo(7001);
        assertThat(ep.toString()).isEqualTo("localhost:7001");

        Endpoint parsed = Endpoint.from("127.0.0.1:8080");
        assertThat(parsed.host()).isEqualTo("127.0.0.1");
        assertThat(parsed.port()).isEqualTo(8080);

        assertThatThrownBy(() -> Endpoint.of("", 7001))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Endpoint.of("localhost", -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Endpoint.of("localhost", 70000))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNodeAndClusterConfiguration() {
        NodeId node1 = NodeId.of("node-1");
        Endpoint ep1 = Endpoint.of("localhost", 7001);

        NodeConfiguration nodeConfig = NodeConfiguration.builder()
                .nodeId(node1)
                .endpoint(ep1)
                .electionTimeout(Duration.ofMillis(500))
                .heartbeatInterval(Duration.ofMillis(150))
                .build();

        assertThat(nodeConfig.nodeId()).isEqualTo(node1);
        assertThat(nodeConfig.endpoint()).isEqualTo(ep1);
        assertThat(nodeConfig.electionTimeout()).isEqualTo(Duration.ofMillis(500));

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("test-cluster")
                .addMember(node1, ep1)
                .addMember("node-2", "localhost", 7002)
                .build();

        assertThat(clusterConfig.clusterSize()).isEqualTo(2);
        assertThat(clusterConfig.contains(node1)).isTrue();
        assertThat(clusterConfig.getEndpoint(node1)).contains(ep1);
    }
}
