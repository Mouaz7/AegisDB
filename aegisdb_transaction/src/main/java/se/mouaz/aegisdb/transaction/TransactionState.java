package se.mouaz.aegisdb.transaction;

/**
 * Lifecycle states of a transaction (Master Project Plan §9 & §18).
 * State Machine:
 * ACTIVE -> PREPARING -> PREPARED -> COMMITTED
 *       \-> ABORTED
 */
public enum TransactionState {
    ACTIVE,
    PREPARING,
    PREPARED,
    COMMITTED,
    ABORTED;

    public boolean isTerminal() {
        return this == COMMITTED || this == ABORTED;
    }

    public boolean canTransitionTo(TransactionState next) {
        if (this == next) {
            return true; // idempotent self-transition
        }
        return switch (this) {
            case ACTIVE -> next == PREPARING || next == ABORTED;
            case PREPARING -> next == PREPARED || next == COMMITTED || next == ABORTED;
            case PREPARED -> next == COMMITTED || next == ABORTED;
            case COMMITTED, ABORTED -> false;
        };
    }
}
