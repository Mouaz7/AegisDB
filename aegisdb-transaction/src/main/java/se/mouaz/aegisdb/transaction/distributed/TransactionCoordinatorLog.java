package se.mouaz.aegisdb.transaction.distributed;

import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Journaling contract for logging 2PC coordinator state transitions and recovery (Master Project Plan §10; US015, Milestone M4 Gate).
 */
public interface TransactionCoordinatorLog extends Closeable {

    /**
     * Appends a state transition to the coordinator log.
     */
    void logState(TransactionId txId, TwoPhaseCommitState state, Set<ShardId> participants);

    /**
     * Scans the log and recovers the latest state for all tracked transactions.
     */
    Map<TransactionId, CoordinatorLogEntry> recover();

    /**
     * Retrieves the current logged state for a specific transaction.
     */
    Optional<CoordinatorLogEntry> getEntry(TransactionId txId);

    @Override
    void close();
}
