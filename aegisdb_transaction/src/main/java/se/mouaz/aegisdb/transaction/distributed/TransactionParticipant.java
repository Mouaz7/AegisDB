package se.mouaz.aegisdb.transaction.distributed;

import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Contract for a shard participant in the Two-Phase Commit protocol (Master Project Plan §10; US015, Milestone M4 Gate).
 */
public interface TransactionParticipant {

    /**
     * Executes Phase 1 (PREPARE): validates read/write conflicts, acquires prepare locks,
     * and votes PREPARED (YES) or ABORT (NO).
     */
    CompletableFuture<ParticipantVote> prepare(PrepareRequest request);

    /**
     * Executes Phase 2 (COMMIT): writes mutations permanently to the shard store,
     * releases prepare locks, and marks transaction COMMITTED.
     */
    CompletableFuture<Void> commit(TransactionId txId);

    /**
     * Executes Phase 2 (ABORT): discards in-doubt writes and releases prepare locks.
     */
    CompletableFuture<Void> abort(TransactionId txId);

    /**
     * Returns the ShardId governed by this participant.
     */
    ShardId shardId();

    /**
     * Queries the in-doubt prepared status of a transaction on this shard.
     */
    Optional<ParticipantVote> getPreparedState(TransactionId txId);
}
