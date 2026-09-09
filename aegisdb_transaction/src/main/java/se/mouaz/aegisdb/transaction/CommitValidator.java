package se.mouaz.aegisdb.transaction;

import se.mouaz.aegisdb.mvcc.MvccStore;

import java.util.Objects;

/**
 * Validates whether a transaction satisfies all preconditions and isolation invariants to commit (Master Project Plan §9).
 */
public class CommitValidator {
    private final ConflictDetector conflictDetector;
    private final TransactionConfig config;

    public CommitValidator(ConflictDetector conflictDetector, TransactionConfig config) {
        this.conflictDetector = Objects.requireNonNull(conflictDetector, "conflictDetector must not be null");
        this.config = Objects.requireNonNull(config, "config must not be null");
    }

    public CommitValidator(TransactionConfig config) {
        this(new ConflictDetector(), config);
    }

    public void validate(TransactionContext context, MvccStore store) {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(store, "store must not be null");

        TransactionState state = context.state();
        if (state != TransactionState.ACTIVE && state != TransactionState.PREPARING && state != TransactionState.PREPARED) {
            throw new IllegalStateException("Cannot validate transaction in state: " + state);
        }

        // Validate timeout
        long now = System.currentTimeMillis();
        long ttlMillis = config.transactionTtl().toMillis();
        if (context.isExpired(now, ttlMillis)) {
            throw new TransactionTimeoutException(
                    context.id().value(),
                    now - context.createdWallClockMillis(),
                    ttlMillis
            );
        }

        // Validate write conflicts (Snapshot Isolation & First-Committer-Wins)
        conflictDetector.validateWriteConflicts(context, store);

        // Validate serializable read conflicts (Anti-dependency checking)
        conflictDetector.validateSerializableConflicts(context, store);
    }

    public ConflictDetector conflictDetector() {
        return conflictDetector;
    }
}
