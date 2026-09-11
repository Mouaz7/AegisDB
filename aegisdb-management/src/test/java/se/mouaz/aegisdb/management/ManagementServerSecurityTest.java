package se.mouaz.aegisdb.management;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.management.security.ManagementSecurityManager;
import se.mouaz.aegisdb.management.security.RateLimiter;
import se.mouaz.aegisdb.management.security.SecurityGuardrails;
import se.mouaz.aegisdb.node.DatabaseNode;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ManagementServerSecurityTest {

    @TempDir
    Path tempDir;

    private ManagementHttpServer server;
    private HttpClient httpClient;
    private String adminToken;
    private String monitorToken;
    private RateLimiter rateLimiter;

    @BeforeEach
    void setUp() throws Exception {
        adminToken = "admin-secret-token-12345";
        monitorToken = "monitor-secret-token-67890";

        NodeId nodeId = NodeId.of("node-mgmt-test");
        InMemoryTransport transport = new InMemoryTransport(nodeId);
        transport.start();

        NodeConfiguration nodeConfig = NodeConfiguration.builder()
                .nodeId(nodeId)
                .endpoint(new Endpoint("localhost", 9090))
                .dataDir(tempDir)
                .build();
        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId(new se.mouaz.aegisdb.common.ClusterId("test-cluster"))
                .build();
        DatabaseNode node = new DatabaseNode(nodeConfig, clusterConfig, transport);
        node.start();

        ManagementSecurityManager securityManager = new ManagementSecurityManager(adminToken, monitorToken, true);
        rateLimiter = new RateLimiter(5.0, 5.0); // 5 req/sec for testing rate limit rejection

        // Use ephemeral port (port 0)
        server = new ManagementHttpServer(0, node, null, securityManager, rateLimiter);
        server.start();

        httpClient = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    private String baseUri() {
        return "http://localhost:" + server.port();
    }

    @Test
    @DisplayName("US017 AC4: Unauthenticated request to /health returns HTTP 401 Unauthorized")
    void unauthenticatedRequestReturns401() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseUri() + "/health"))
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("Unauthorized");
    }

    @Test
    @DisplayName("US017 AC4: Monitor role can access /health and /node but gets 403 Forbidden on /admin/snapshot")
    void monitorRoleAccessAndForbiddenAdmin() throws Exception {
        // Access /health with Monitor token
        HttpRequest healthReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUri() + "/health"))
                .header("Authorization", "Bearer " + monitorToken)
                .GET()
                .build();

        HttpResponse<String> healthResp = httpClient.send(healthReq, HttpResponse.BodyHandlers.ofString());
        assertThat(healthResp.statusCode()).isEqualTo(200);
        assertThat(healthResp.body()).contains("\"status\":\"UP\"");

        // Attempt /admin/snapshot with Monitor token -> should be 403 Forbidden
        HttpRequest adminReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUri() + "/admin/snapshot"))
                .header("Authorization", "Bearer " + monitorToken)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> adminResp = httpClient.send(adminReq, HttpResponse.BodyHandlers.ofString());
        assertThat(adminResp.statusCode()).isEqualTo(403);
        assertThat(adminResp.body()).contains("Forbidden");
    }

    @Test
    @DisplayName("US017 AC4: Admin role can execute /admin/snapshot successfully")
    void adminRoleCanExecuteAdminEndpoints() throws Exception {
        HttpRequest adminReq = HttpRequest.newBuilder()
                .uri(URI.create(baseUri() + "/admin/snapshot"))
                .header("Authorization", "Bearer " + adminToken)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<String> adminResp = httpClient.send(adminReq, HttpResponse.BodyHandlers.ofString());
        assertThat(adminResp.statusCode()).isEqualTo(200);
        assertThat(adminResp.body()).contains("Snapshot triggered successfully");
    }

    @Test
    @DisplayName("US017 AC3: Rate limiter throttles rapid successive requests with HTTP 429")
    void rateLimiterThrottlesRequests() throws Exception {
        int accepted = 0;
        int rateLimited = 0;

        for (int i = 0; i < 15; i++) {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUri() + "/health"))
                    .header("Authorization", "Bearer " + monitorToken)
                    .GET()
                    .build();

            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) {
                accepted++;
            } else if (resp.statusCode() == 429) {
                rateLimited++;
            }
        }

        assertThat(accepted).isGreaterThan(0);
        assertThat(rateLimited).isGreaterThan(0);
    }

    @Test
    @DisplayName("US017 AC3: SecurityGuardrails strictly bounds input sizes and blocks path traversal")
    void inputBoundingAndPathTraversalDefense() {
        // 1. Key bounding: Reject keys > 1024 bytes
        String validKey = "valid-key-123";
        SecurityGuardrails.validateKey(validKey);

        String oversizedKey = "A".repeat(1025);
        assertThatThrownBy(() -> SecurityGuardrails.validateKey(oversizedKey))
                .isInstanceOf(SecurityGuardrails.SecurityValidationException.class)
                .hasMessageContaining("exceeds maximum permitted limit");

        // 2. Value bounding: Reject values > 16 MB
        byte[] validValue = new byte[100];
        SecurityGuardrails.validateValue(validValue);

        byte[] oversizedValue = new byte[16 * 1024 * 1024 + 1];
        assertThatThrownBy(() -> SecurityGuardrails.validateValue(oversizedValue))
                .isInstanceOf(SecurityGuardrails.SecurityValidationException.class)
                .hasMessageContaining("exceeds maximum permitted limit");

        // 3. Path traversal attack defense: Reject path escaping base directory
        Path basePath = tempDir.resolve("data");
        String illegalTraversal = "../../etc/passwd";

        assertThatThrownBy(() -> SecurityGuardrails.sanitizeAndResolvePath(basePath, illegalTraversal))
                .isInstanceOf(SecurityGuardrails.SecurityValidationException.class)
                .hasMessageContaining("Path traversal attempt detected");
    }
}
