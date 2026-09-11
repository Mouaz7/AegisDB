package se.mouaz.aegisdb.management.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;

/**
 * Manages authentication and RBAC authorization for the AegisDB Management API.
 * Uses constant-time token comparison (MessageDigest.isEqual) to defend against timing attacks.
 */
public class ManagementSecurityManager {
    private static final Logger log = LoggerFactory.getLogger(ManagementSecurityManager.class);
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final String adminToken;
    private final String monitorToken;
    private final boolean requireAuthOnHealth;

    public ManagementSecurityManager(String adminToken, String monitorToken, boolean requireAuthOnHealth) {
        this.adminToken = Objects.requireNonNull(adminToken, "adminToken cannot be null");
        this.monitorToken = Objects.requireNonNull(monitorToken, "monitorToken cannot be null");
        this.requireAuthOnHealth = requireAuthOnHealth;
    }

    public ManagementSecurityManager(String adminToken, String monitorToken) {
        this(adminToken, monitorToken, false);
    }

    public static String generateSecureToken() {
        byte[] bytes = new byte[32];
        SECURE_RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    /**
     * Authenticates an incoming Authorization header (Bearer token) or API key.
     * Performs constant-time comparison to prevent side-channel timing attacks.
     */
    public Optional<SecurityPrincipal> authenticate(String authHeader) {
        if (authHeader == null || authHeader.isBlank()) {
            return Optional.empty();
        }

        String token = authHeader.trim();
        if (token.regionMatches(true, 0, "Bearer ", 0, 7)) {
            token = token.substring(7).trim();
        }

        byte[] provided = token.getBytes(StandardCharsets.UTF_8);
        byte[] adminExpected = adminToken.getBytes(StandardCharsets.UTF_8);
        byte[] monitorExpected = monitorToken.getBytes(StandardCharsets.UTF_8);

        if (MessageDigest.isEqual(provided, adminExpected)) {
            return Optional.of(SecurityPrincipal.admin("admin-user"));
        } else if (MessageDigest.isEqual(provided, monitorExpected)) {
            return Optional.of(SecurityPrincipal.monitor("monitor-user"));
        }

        log.warn("Authentication failed: invalid or unknown token provided");
        return Optional.empty();
    }

    /**
     * Authorizes an authenticated principal for a given URI path and HTTP method.
     */
    public boolean authorize(SecurityPrincipal principal, String path, String method) {
        if ("/health".equalsIgnoreCase(path) && !requireAuthOnHealth) {
            return true;
        }

        if (principal == null || principal.hasRole(Role.ROLE_ANONYMOUS)) {
            return false;
        }

        if (path.startsWith("/admin")) {
            return principal.hasRole(Role.ROLE_ADMIN);
        }

        // Monitoring and diagnostic endpoints require at least ROLE_MONITOR
        return principal.hasRole(Role.ROLE_MONITOR) || principal.hasRole(Role.ROLE_ADMIN);
    }
}
