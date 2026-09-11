package se.mouaz.aegisdb.transaction.distributed;

import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Durable state entry persisted in the TransactionCoordinatorLog (Master Project Plan §10; US015).
 */
public record CoordinatorLogEntry(
        TransactionId txId,
        TwoPhaseCommitState state,
        Set<ShardId> participants,
        long timestamp) {

    public CoordinatorLogEntry {
        Objects.requireNonNull(txId, "txId cannot be null");
        Objects.requireNonNull(state, "state cannot be null");
        participants = participants != null ? Collections.unmodifiableSet(new LinkedHashSet<>(participants)) : Collections.emptySet();
    }
}
