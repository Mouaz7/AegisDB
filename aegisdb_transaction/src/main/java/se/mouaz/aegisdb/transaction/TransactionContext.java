package se.mouaz.aegisdb.transaction;

import se.mouaz.aegisdb.common.ClientId;
import se.mouaz.aegisdb.common.RequestId;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.mvcc.Snapshot;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Encapsulated runtime context of an active, prepared, or completed transaction (Master Project Plan §9).
 */
public class TransactionContext {
    private final TransactionId id;
    private final IsolationLevel isolationLevel;
    private final long startTimestamp;
    private final Snapshot snapshot;
    private final ReadSet readSet = new ReadSet();
    private final WriteSet writeSet = new WriteSet();
    private final AtomicReference<TransactionState> state = new AtomicReference<>(TransactionState.ACTIVE);
    private final long createdWallClockMillis;
    private final ClientId clientId;
    private final RequestId requestId;
    private volatile long commitTimestamp = -1L;

    public TransactionContext(TransactionId id,
                              IsolationLevel isolationLevel,
                              long startTimestamp,
                              Snapshot snapshot,
                              ClientId clientId,
                              RequestId requestId) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.isolationLevel = Objects.requireNonNull(isolationLevel, "isolationLevel must not be null");
        this.startTimestamp = startTimestamp;
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot must not be null");
        this.clientId = clientId;
        this.requestId = requestId;
        this.createdWallClockMillis = System.currentTimeMillis();
    }

    public TransactionId id() {
        return id;
    }

    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    public long startTimestamp() {
        return startTimestamp;
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    public ReadSet readSet() {
        return readSet;
    }

    public WriteSet writeSet() {
        return writeSet;
    }

    public TransactionState state() {
        return state.get();
    }

    public long commitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public Optional<ClientId> clientId() {
        return Optional.ofNullable(clientId);
    }

    public Optional<RequestId> requestId() {
        return Optional.ofNullable(requestId);
    }

    public long createdWallClockMillis() {
        return createdWallClockMillis;
    }

    public boolean isExpired(long nowMillis, long ttlMillis) {
        return (nowMillis - createdWallClockMillis) > ttlMillis && !state.get().isTerminal();
    }

    /**
     * Attempts atomic transition to next state, enforcing state machine rules.
     */
    public boolean transitionTo(TransactionState nextState) {
        while (true) {
            TransactionState current = state.get();
            if (current == nextState) {
                return true; // idempotent
            }
            if (!current.canTransitionTo(nextState)) {
                throw new IllegalStateException(String.format(
                        "Illegal transaction state transition for %s: cannot move from %s to %s",
                        id, current, nextState));
            }
            if (state.compareAndSet(current, nextState)) {
                return true;
            }
        }
    }
}
