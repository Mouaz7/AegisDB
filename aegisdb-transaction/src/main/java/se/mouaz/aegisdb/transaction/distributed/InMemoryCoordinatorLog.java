package se.mouaz.aegisdb.transaction.distributed;

import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of TransactionCoordinatorLog for fast, deterministic unit and simulation tests.
 */
public class InMemoryCoordinatorLog implements TransactionCoordinatorLog {

    private final Map<TransactionId, CoordinatorLogEntry> entries = new ConcurrentHashMap<>();
    private final List<CoordinatorLogEntry> appendHistory = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void logState(TransactionId txId, TwoPhaseCommitState state, Set<ShardId> participants) {
        Objects.requireNonNull(txId, "txId cannot be null");
        Objects.requireNonNull(state, "state cannot be null");
        CoordinatorLogEntry entry = new CoordinatorLogEntry(txId, state, participants, System.currentTimeMillis());
        entries.put(txId, entry);
        appendHistory.add(entry);
    }

    @Override
    public Map<TransactionId, CoordinatorLogEntry> recover() {
        return Collections.unmodifiableMap(new HashMap<>(entries));
    }

    @Override
    public Optional<CoordinatorLogEntry> getEntry(TransactionId txId) {
        return Optional.ofNullable(entries.get(txId));
    }

    public List<CoordinatorLogEntry> appendHistory() {
        return Collections.unmodifiableList(new ArrayList<>(appendHistory));
    }

    @Override
    public void close() {
        // No-op for in-memory log
    }
}
