package se.mouaz.aegisdb.transaction.distributed;

/**
 * State machine for Two-Phase Commit coordinator (Master Project Plan §10; US015, Milestone M4 Gate).
 * Enforces strict transitions:
 * INIT -> PREPARING -> COMMIT_DECIDED -> COMMITTED
 *                    \-> ABORT_DECIDED  -> ABORTED
 */
public enum TwoPhaseCommitState {
    INIT,
    PREPARING,
    COMMIT_DECIDED,
    ABORT_DECIDED,
    COMMITTED,
    ABORTED;

    public boolean isTerminal() {
        return this == COMMITTED || this == ABORTED;
    }

    public boolean isDecided() {
        return this == COMMIT_DECIDED || this == ABORT_DECIDED || isTerminal();
    }
}
