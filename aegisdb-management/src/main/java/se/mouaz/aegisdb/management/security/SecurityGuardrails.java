package se.mouaz.aegisdb.management.security;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Security controls enforcing input validation boundaries, resource exhaustion limits,
 * and path traversal sanitization (Master Plan §12 Secure-by-Design).
 */
public final class SecurityGuardrails {

    public static final int MAX_KEY_SIZE_BYTES = 1024; // 1 KB
    public static final int MAX_VALUE_SIZE_BYTES = 16 * 1024 * 1024; // 16 MB
    public static final int MAX_TRANSACTION_BATCH_SIZE = 10_000;
    public static final long MAX_TRANSACTION_LIFETIME_MS = 60_000; // 60 seconds

    private SecurityGuardrails() {
    }

    /**
     * Validates key length boundaries.
     */
    public static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new SecurityValidationException("Key cannot be null or empty");
        }
        byte[] bytes = key.getBytes();
        if (bytes.length > MAX_KEY_SIZE_BYTES) {
            throw new SecurityValidationException(String.format(
                    "Key size (%d bytes) exceeds maximum permitted limit (%d bytes)",
                    bytes.length, MAX_KEY_SIZE_BYTES));
        }
    }

    /**
     * Validates payload value size boundaries.
     */
    public static void validateValue(byte[] value) {
        if (value != null && value.length > MAX_VALUE_SIZE_BYTES) {
            throw new SecurityValidationException(String.format(
                    "Value payload size (%d bytes) exceeds maximum permitted limit (%d bytes)",
                    value.length, MAX_VALUE_SIZE_BYTES));
        }
    }

    /**
     * Validates transaction batch size boundaries.
     */
    public static void validateBatchSize(int batchSize) {
        if (batchSize < 0) {
            throw new SecurityValidationException("Batch size cannot be negative");
        }
        if (batchSize > MAX_TRANSACTION_BATCH_SIZE) {
            throw new SecurityValidationException(String.format(
                    "Transaction batch size (%d) exceeds maximum permitted limit (%d)",
                    batchSize, MAX_TRANSACTION_BATCH_SIZE));
        }
    }

    /**
     * Sanitizes and validates that a relative storage subpath does not escape the configured base directory.
     * Prevents Directory Traversal (ASVS V12.3 / OWASP Top 10).
     */
    public static Path sanitizeAndResolvePath(Path baseDirectory, String candidateSubPath) {
        Objects.requireNonNull(baseDirectory, "baseDirectory cannot be null");
        if (candidateSubPath == null || candidateSubPath.isBlank()) {
            throw new SecurityValidationException("Candidate path cannot be null or blank");
        }

        if (candidateSubPath.contains("\0")) {
            throw new SecurityValidationException("Path contains forbidden null byte character");
        }

        Path basePathNormalized = baseDirectory.toAbsolutePath().normalize();
        Path candidatePath = Paths.get(candidateSubPath);

        Path resolved = candidatePath.isAbsolute()
                ? candidatePath.normalize()
                : basePathNormalized.resolve(candidatePath).normalize();

        if (!resolved.startsWith(basePathNormalized)) {
            throw new SecurityValidationException(String.format(
                    "Path traversal attempt detected: '%s' escapes base directory '%s'",
                    candidateSubPath, basePathNormalized));
        }

        return resolved;
    }

    public static class SecurityValidationException extends IllegalArgumentException {
        public SecurityValidationException(String message) {
            super(message);
        }
    }
}
