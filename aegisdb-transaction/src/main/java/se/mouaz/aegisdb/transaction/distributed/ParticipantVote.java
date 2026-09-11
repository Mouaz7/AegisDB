package se.mouaz.aegisdb.transaction.distributed;

/**
 * Vote returned by a shard participant during Phase 1 (PREPARE) of Two-Phase Commit (Master Project Plan §10; US015).
 */
public enum ParticipantVote {
    PREPARED,
    ABORT;

    public boolean isPrepared() {
        return this == PREPARED;
    }
}
