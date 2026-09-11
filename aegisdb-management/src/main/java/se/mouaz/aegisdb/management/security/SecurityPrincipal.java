package se.mouaz.aegisdb.management.security;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/**
 * Represents an authenticated management caller with assigned RBAC roles.
 */
public record SecurityPrincipal(String name, Set<Role> roles, Instant authenticatedAt) {
    public SecurityPrincipal {
        Objects.requireNonNull(name, "name cannot be null");
        Objects.requireNonNull(roles, "roles cannot be null");
        Objects.requireNonNull(authenticatedAt, "authenticatedAt cannot be null");
    }

    public static SecurityPrincipal anonymous() {
        return new SecurityPrincipal("anonymous", Set.of(Role.ROLE_ANONYMOUS), Instant.now());
    }

    public static SecurityPrincipal monitor(String name) {
        return new SecurityPrincipal(name, Set.of(Role.ROLE_MONITOR), Instant.now());
    }

    public static SecurityPrincipal admin(String name) {
        return new SecurityPrincipal(name, Set.of(Role.ROLE_MONITOR, Role.ROLE_ADMIN), Instant.now());
    }

    public boolean hasRole(Role role) {
        return roles.contains(role);
    }
}
