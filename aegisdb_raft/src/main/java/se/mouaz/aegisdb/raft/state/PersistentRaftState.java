package se.mouaz.aegisdb.raft.state;

import se.mouaz.aegisdb.common.NodeId;

import java.util.Optional;

/**
 * Persistent Raft state (Section 16).
 * Must survive crashes.
 */
public class PersistentRaftState {
    private long currentTerm;
    private NodeId votedFor;

    public PersistentRaftState(long initialTerm, NodeId votedFor) {
        if (initialTerm < 0) {
            throw new IllegalArgumentException("Term cannot be negative: " + initialTerm);
        }
        this.currentTerm = initialTerm;
        this.votedFor = votedFor;
    }

    public PersistentRaftState() {
        this(0L, null);
    }

    public synchronized long currentTerm() {
        return currentTerm;
    }

    public synchronized void setCurrentTerm(long newTerm) {
        RaftInvariants.checkTermNeverDecreases(this.currentTerm, newTerm);
        if (newTerm > this.currentTerm) {
            this.currentTerm = newTerm;
            this.votedFor = null;
        }
    }

    public synchronized Optional<NodeId> votedFor() {
        return Optional.ofNullable(votedFor);
    }

    public synchronized void setVotedFor(NodeId candidateId) {
        this.votedFor = candidateId;
    }

    public synchronized void updateTermAndVote(long newTerm, NodeId candidateId) {
        RaftInvariants.checkTermNeverDecreases(this.currentTerm, newTerm);
        this.currentTerm = newTerm;
        this.votedFor = candidateId;
    }
}
