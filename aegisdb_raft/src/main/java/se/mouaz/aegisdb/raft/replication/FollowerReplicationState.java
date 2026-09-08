package se.mouaz.aegisdb.raft.replication;

import se.mouaz.aegisdb.common.NodeId;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks replication state for a specific follower from the leader's perspective (Section 83; Ongaro §5.3).
 */
public class FollowerReplicationState {
    private final NodeId peerId;
    private final AtomicLong nextIndex = new AtomicLong(1);
    private final AtomicLong matchIndex = new AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicBoolean inFlight = new java.util.concurrent.atomic.AtomicBoolean(false);

    public FollowerReplicationState(NodeId peerId, long initialNextIndex) {
        this.peerId = Objects.requireNonNull(peerId, "peerId cannot be null");
        this.nextIndex.set(Math.max(1, initialNextIndex));
        this.matchIndex.set(0);
    }

    public boolean tryStartRpc() {
        return inFlight.compareAndSet(false, true);
    }

    public void finishRpc() {
        inFlight.set(false);
    }

    public NodeId peerId() {
        return peerId;
    }

    public long nextIndex() {
        return nextIndex.get();
    }

    public void setNextIndex(long index) {
        this.nextIndex.set(Math.max(1, index));
    }

    public long matchIndex() {
        return matchIndex.get();
    }

    public void setMatchIndex(long index) {
        this.matchIndex.set(Math.max(0, index));
    }

    public void recordSuccess(long ackIndex) {
        this.matchIndex.updateAndGet(curr -> Math.max(curr, ackIndex));
        this.nextIndex.set(this.matchIndex.get() + 1);
    }

    public void decrementNextIndex(long matchHint) {
        if (matchHint > 0) {
            nextIndex.updateAndGet(curr -> Math.max(1, Math.min(curr - 1, matchHint + 1)));
        } else {
            nextIndex.updateAndGet(curr -> Math.max(1, curr - 1));
        }
    }

    @Override
    public String toString() {
        return "FollowerReplicationState{" +
                "peerId=" + peerId +
                ", nextIndex=" + nextIndex.get() +
                ", matchIndex=" + matchIndex.get() +
                '}';
    }
}
