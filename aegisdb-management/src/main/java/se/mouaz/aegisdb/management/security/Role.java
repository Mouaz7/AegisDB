package se.mouaz.aegisdb.management.security;

/**
 * Role-Based Access Control (RBAC) roles for the AegisDB Management API.
 */
public enum Role {
    /**
     * Unauthenticated or guest access.
     */
    ROLE_ANONYMOUS,

    /**
     * Read-only operational role with access to health, metrics, cluster status, and shard info.
     */
    ROLE_MONITOR,

    /**
     * Privileged administrative role with permission to execute state mutations,
     * snapshots, step-downs, and chaos injection.
     */
    ROLE_ADMIN
}
