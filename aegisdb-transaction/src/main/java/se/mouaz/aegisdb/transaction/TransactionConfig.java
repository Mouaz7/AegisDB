package se.mouaz.aegisdb.transaction;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for transaction bounds, timeouts, and isolation defaults (Master Project Plan §12).
 */
public record TransactionConfig(
        int maxWriteSetSize,
        Duration transactionTtl,
        IsolationLevel defaultIsolationLevel,
        int defaultMaxRetries
) {
    public static final int DEFAULT_MAX_WRITE_SET_SIZE = 10_000;
    public static final Duration DEFAULT_TRANSACTION_TTL = Duration.ofSeconds(30);
    public static final IsolationLevel DEFAULT_ISOLATION_LEVEL = IsolationLevel.SNAPSHOT_ISOLATION;
    public static final int DEFAULT_MAX_RETRIES = 5;

    public TransactionConfig {
        if (maxWriteSetSize <= 0) {
            throw new IllegalArgumentException("maxWriteSetSize must be positive, got: " + maxWriteSetSize);
        }
        Objects.requireNonNull(transactionTtl, "transactionTtl must not be null");
        if (transactionTtl.isNegative() || transactionTtl.isZero()) {
            throw new IllegalArgumentException("transactionTtl must be positive, got: " + transactionTtl);
        }
        Objects.requireNonNull(defaultIsolationLevel, "defaultIsolationLevel must not be null");
        if (defaultMaxRetries < 0) {
            throw new IllegalArgumentException("defaultMaxRetries must be non-negative, got: " + defaultMaxRetries);
        }
    }

    public static TransactionConfig defaultConfig() {
        return new TransactionConfig(
                DEFAULT_MAX_WRITE_SET_SIZE,
                DEFAULT_TRANSACTION_TTL,
                DEFAULT_ISOLATION_LEVEL,
                DEFAULT_MAX_RETRIES
        );
    }
}
