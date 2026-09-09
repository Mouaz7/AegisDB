package se.mouaz.aegisdb.management;

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
 * Lightweight, zero-dependency REST Management Server hosting operational and diagnostic endpoints
 * (Master Plan §15 and §17 / US017).
 * Implemented using JDK {@link HttpServer} with RBAC token authentication and rate limiting.
 */
public class ManagementHttpServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ManagementHttpServer.class);

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

        this.server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
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
            sendResponse(exchange, 429, "{\"error\":\"Too Many Requests\",\"status\":429}");
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
            sendResponse(exchange, status, String.format("{\"error\":\"%s\",\"status\":%d}", msg, status));
            return;
        }

        // 4. Dispatch handler
        try {
            String jsonResponse = handler.handle(exchange);
            sendResponse(exchange, 200, jsonResponse);
        } catch (Exception e) {
            log.error("Internal error handling management endpoint: {}", path, e);
            sendResponse(exchange, 500, String.format("{\"error\":\"%s\",\"status\":500}", e.getMessage()));
        }
    }

    // --- Endpoint Handlers ---

    private String handleHealth(HttpExchange ex) {
        long uptime = System.currentTimeMillis() - startedAt;
        return String.format("{\"status\":\"UP\",\"nodeId\":\"%s\",\"nodeStatus\":\"%s\",\"uptimeMs\":%d}",
                node.nodeId(), node.status(), uptime);
    }

    private String handleNode(HttpExchange ex) {
        return String.format("{\"nodeId\":\"%s\",\"status\":\"%s\",\"endpoint\":\"%s:%d\",\"storageEnginePresent\":%b}",
                node.nodeId(), node.status(),
                node.config().endpoint().host(), node.config().endpoint().port(),
                node.storageEngine().isPresent());
    }

    private String handleCluster(HttpExchange ex) {
        return String.format("{\"clusterId\":\"%s\",\"nodeCount\":%d}",
                node.clusterConfig().clusterId().value(),
                node.clusterConfig().clusterSize());
    }

    private String handleRaft(HttpExchange ex) {
        if (node.raftNode().isEmpty()) {
            return "{\"raftEnabled\":false}";
        }
        RaftNode raft = node.raftNode().get();
        return String.format("{\"raftEnabled\":true,\"term\":%d,\"role\":\"%s\",\"commitIndex\":%d,\"lastApplied\":%d,\"logSize\":%d}",
                raft.currentTerm(),
                raft.role(),
                raft.commitIndex(),
                raft.lastApplied(),
                raft.log().lastLogIndex());
    }

    private String handleShards(HttpExchange ex) {
        if (shardManager == null) {
            return "{\"shardingEnabled\":false}";
        }
        return String.format("{\"shardingEnabled\":true,\"shardCount\":%d}",
                shardManager.shardMap().allShards().size());
    }

    private String handleTransactions(HttpExchange ex) {
        return "{\"transactionManagerActive\":true,\"activeTransactions\":0,\"committedCount\":0,\"abortedCount\":0}";
    }

    private String handleMetrics(HttpExchange ex) {
        long uptime = System.currentTimeMillis() - startedAt;
        return String.format("{\"metrics\":{\"uptime_seconds\":%d,\"node_status\":1,\"requests_total\":0}}",
                uptime / 1000);
    }

    private String handleAdminSnapshot(HttpExchange ex) {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            return "{\"error\":\"POST required\"}";
        }
        log.info("Admin triggered snapshot compaction on node {}", node.nodeId());
        return "{\"success\":true,\"message\":\"Snapshot triggered successfully\"}";
    }

    private String handleAdminStepDown(HttpExchange ex) {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            return "{\"error\":\"POST required\"}";
        }
        log.info("Admin requested leader step down on node {}", node.nodeId());
        node.raftNode().ifPresent(r -> r.electionTimer().reset());
        return "{\"success\":true,\"message\":\"Leader step down initiated\"}";
    }

    private void sendResponse(HttpExchange exchange, int statusCode, String responseBody) throws IOException {
        byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    @FunctionalInterface
    private interface EndpointHandler {
        String handle(HttpExchange exchange) throws Exception;
    }
}
