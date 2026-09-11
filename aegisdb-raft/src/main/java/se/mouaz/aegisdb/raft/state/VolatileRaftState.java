package se.mouaz.aegisdb.raft.state;

/**
 * Volatile Raft state on all servers (Section 17).
 */
public class VolatileRaftState {
    private long commitIndex;
    private long lastApplied;

    public VolatileRaftState(long commitIndex, long lastApplied) {
        this.commitIndex = commitIndex;
        this.lastApplied = lastApplied;
    }

    public VolatileRaftState() {
        this(0L, 0L);
    }

    public synchronized long commitIndex() {
        return commitIndex;
    }

    public synchronized void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public synchronized long lastApplied() {
        return lastApplied;
    }

    public synchronized void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }
}
