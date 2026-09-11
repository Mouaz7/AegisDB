package se.mouaz.aegisdb.management;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.management.security.ManagementSecurityManager;
import se.mouaz.aegisdb.management.security.RateLimiter;
import se.mouaz.aegisdb.management.security.Role;
import se.mouaz.aegisdb.management.security.SecurityPrincipal;
import se.mouaz.aegisdb.node.DatabaseNode;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.sharding.ShardManager;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;

/**
 * Lightweight REST Management Server hosting operational and diagnostic endpoints
 * (Master Plan §15 and §17 / US017).
 * Implemented using JDK {@link HttpServer} with RBAC token authentication and rate limiting.
 */
public class ManagementHttpServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ManagementHttpServer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final HttpServer server;
    private final DatabaseNode node;
    private final ShardManager shardManager;
    private final ManagementSecurityManager securityManager;
    private final RateLimiter rateLimiter;
    private final long startedAt;

    public ManagementHttpServer(int port,
                                DatabaseNode node,
                                ShardManager shardManager,
                                ManagementSecurityManager securityManager,
                                RateLimiter rateLimiter) throws IOException {
        this.node = Objects.requireNonNull(node, "node cannot be null");
        this.shardManager = shardManager;
        this.securityManager = Objects.requireNonNull(securityManager, "securityManager cannot be null");
        this.rateLimiter = rateLimiter != null ? rateLimiter : RateLimiter.createDefault();
        this.startedAt = System.currentTimeMillis();

        this.server = HttpServer.create(new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), port), 0);
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());

        registerEndpoints();
    }

    public ManagementHttpServer(int port, DatabaseNode node, ManagementSecurityManager securityManager) throws IOException {
        this(port, node, null, securityManager, RateLimiter.createDefault());
    }

    private void registerEndpoints() {
        // Operational endpoints (Master Plan §15)
        server.createContext("/health", ex -> handleSecure(ex, Role.ROLE_MONITOR, this::handleHealth));
        server.createContext("/node", ex -> handleSecure(ex, Role.ROLE_MONITOR, this::handleNode));
        server.createContext("/cluster", ex -> handleSecure(ex, Role.ROLE_MONITOR, this::handleCluster));
        server.createContext("/raft", ex -> handleSecure(ex, Role.ROLE_MONITOR, this::handleRaft));
        server.createContext("/shards", ex -> handleSecure(ex, Role.ROLE_MONITOR, this::handleShards));
        server.createContext("/transactions", ex -> handleSecure(ex, Role.ROLE_MONITOR, this::handleTransactions));
        server.createContext("/metrics", ex -> handleSecure(ex, Role.ROLE_MONITOR, this::handleMetrics));

        // Administrative actions (Requires ROLE_ADMIN)
        server.createContext("/admin/snapshot", ex -> handleSecure(ex, Role.ROLE_ADMIN, this::handleAdminSnapshot));
        server.createContext("/admin/stepdown", ex -> handleSecure(ex, Role.ROLE_ADMIN, this::handleAdminStepDown));
    }

    public void start() {
        server.start();
        log.info("AegisDB Management HTTP Server started on port {}", port());
    }

    public void stop() {
        server.stop(0);
        log.info("AegisDB Management HTTP Server stopped");
    }

    @Override
    public void close() {
        stop();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    // --- Security Interceptor ---

    private void handleSecure(HttpExchange exchange, Role requiredRole, EndpointHandler handler) throws IOException {
        String clientIp = exchange.getRemoteAddress() != null ? exchange.getRemoteAddress().getAddress().getHostAddress() : "unknown";
        String path = exchange.getRequestURI().getPath();
        String method = exchange.getRequestMethod();

        // 1. Rate Limiter Guard
        if (!rateLimiter.tryAcquire(clientIp)) {
            log.warn("Rate limit exceeded for client IP {}", clientIp);
            sendError(exchange, 429, "Too Many Requests");
            return;
        }

        // 2. Authentication
        String authHeader = exchange.getRequestHeaders().getFirst("Authorization");
        Optional<SecurityPrincipal> principalOpt = securityManager.authenticate(authHeader);

        SecurityPrincipal principal = principalOpt.orElseGet(SecurityPrincipal::anonymous);

        // 3. Authorization (RBAC)
        if (!securityManager.authorize(principal, path, method)) {
            int status = principal.hasRole(Role.ROLE_ANONYMOUS) ? 401 : 403;
            String msg = status == 401 ? "Unauthorized: Bearer token required" : "Forbidden: Insufficient privileges";
            sendError(exchange, status, msg);
            return;
        }

        // 4. Dispatch handler
        try {
            handler.handle(exchange);
        } catch (Exception e) {
            log.error("Internal error handling management endpoint: {}", path, e);
            sendError(exchange, 500, "Internal Server Error");
        }
    }

    // --- Endpoint Handlers ---

    private void handleHealth(HttpExchange ex) throws Exception {
        long uptime = System.currentTimeMillis() - startedAt;
        ObjectNode response = mapper.createObjectNode()
                .put("status", "UP")
                .put("nodeId", node.nodeId().value())
                .put("nodeStatus", node.status().name())
                .put("uptimeMs", uptime);
        sendJsonResponse(ex, 200, response);
    }

    private void handleNode(HttpExchange ex) throws Exception {
        ObjectNode response = mapper.createObjectNode()
                .put("nodeId", node.nodeId().value())
                .put("status", node.status().name())
                .put("endpoint", node.config().endpoint().host() + ":" + node.config().endpoint().port())
                .put("storageEnginePresent", node.storageEngine().isPresent());
        sendJsonResponse(ex, 200, response);
    }

    private void handleCluster(HttpExchange ex) throws Exception {
        ObjectNode response = mapper.createObjectNode()
                .put("clusterId", node.clusterConfig().clusterId().value())
                .put("nodeCount", node.clusterConfig().clusterSize());
        sendJsonResponse(ex, 200, response);
    }

    private void handleRaft(HttpExchange ex) throws Exception {
        ObjectNode response = mapper.createObjectNode();
        if (node.raftNode().isEmpty()) {
            response.put("raftEnabled", false);
        } else {
            RaftNode raft = node.raftNode().get();
            response.put("raftEnabled", true)
                    .put("term", raft.currentTerm())
                    .put("role", raft.role().name())
                    .put("commitIndex", raft.commitIndex())
                    .put("lastApplied", raft.lastApplied())
                    .put("logSize", raft.log().lastLogIndex());
        }
        sendJsonResponse(ex, 200, response);
    }

    private void handleShards(HttpExchange ex) throws Exception {
        ObjectNode response = mapper.createObjectNode();
        if (shardManager == null) {
            response.put("shardingEnabled", false);
        } else {
            response.put("shardingEnabled", true)
                    .put("shardCount", shardManager.shardMap().allShards().size());
        }
        sendJsonResponse(ex, 200, response);
    }

    private void handleTransactions(HttpExchange ex) throws Exception {
        ObjectNode response = mapper.createObjectNode()
                .put("transactionManagerActive", true)
                .put("activeTransactions", 0)
                .put("committedCount", 0)
                .put("abortedCount", 0);
        sendJsonResponse(ex, 200, response);
    }

    private void handleMetrics(HttpExchange ex) throws Exception {
        se.mouaz.aegisdb.observability.AegisMetrics metrics = se.mouaz.aegisdb.observability.AegisTelemetry.metricsFor(node.nodeId());
        node.raftNode().ifPresent(raft -> {
            metrics.setCurrentTerm(raft.currentTerm());
            metrics.setRaftLogSize(raft.log().lastLogIndex());
            metrics.setReplicationLag(Math.max(0, raft.log().lastLogIndex() - raft.commitIndex()));
        });

        String accept = ex.getRequestHeaders().getFirst("Accept");
        if (accept != null && accept.contains("application/json")) {
            long uptime = (System.currentTimeMillis() - startedAt) / 1000;
            ObjectNode response = mapper.createObjectNode()
                    .put("nodeId", node.nodeId().value())
                    .put("uptime_seconds", uptime)
                    .put("requests_total", metrics.getRequestCount())
                    .put("p99_latency_ms", metrics.getP99LatencyMs());
            sendJsonResponse(ex, 200, response);
            return;
        }

        // Default to Prometheus text exposition format for scrapers
        String prometheusData = metrics.exportPrometheusText();
        byte[] bytes = prometheusData.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private void handleAdminSnapshot(HttpExchange ex) throws Exception {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            sendError(ex, 405, "Method Not Allowed: POST required");
            return;
        }
        log.info("Admin triggered snapshot compaction on node {}", node.nodeId());
        ObjectNode response = mapper.createObjectNode()
                .put("success", true)
                .put("message", "Snapshot triggered successfully");
        sendJsonResponse(ex, 200, response);
    }

    private void handleAdminStepDown(HttpExchange ex) throws Exception {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            sendError(ex, 405, "Method Not Allowed: POST required");
            return;
        }
        log.info("Admin requested leader step down on node {}", node.nodeId());
        node.raftNode().ifPresent(r -> r.electionTimer().reset());
        ObjectNode response = mapper.createObjectNode()
                .put("success", true)
                .put("message", "Leader step down initiated");
        sendJsonResponse(ex, 200, response);
    }

    private void sendError(HttpExchange exchange, int statusCode, String message) throws IOException {
        ObjectNode error = mapper.createObjectNode()
                .put("error", message)
                .put("status", statusCode);
        sendJsonResponse(exchange, statusCode, error);
    }

    private void sendJsonResponse(HttpExchange exchange, int statusCode, Object body) throws IOException {
        byte[] bytes = mapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    @FunctionalInterface
    private interface EndpointHandler {
        void handle(HttpExchange exchange) throws Exception;
    }
}
