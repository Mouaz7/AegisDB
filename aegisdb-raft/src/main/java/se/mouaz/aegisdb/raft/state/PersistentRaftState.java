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

    @FunctionalInterface
    public interface StatePersistenceListener {
        void onStateChanged(long term, NodeId votedFor);
    }

    private StatePersistenceListener persistenceListener;

    public PersistentRaftState(long initialTerm, NodeId votedFor, StatePersistenceListener persistenceListener) {
        if (initialTerm < 0) {
            throw new IllegalArgumentException("Term cannot be negative: " + initialTerm);
        }
        this.currentTerm = initialTerm;
        this.votedFor = votedFor;
        this.persistenceListener = persistenceListener;
    }

    public PersistentRaftState(long initialTerm, NodeId votedFor) {
        this(initialTerm, votedFor, null);
    }

    public PersistentRaftState() {
        this(0L, null, null);
    }

    public synchronized void setPersistenceListener(StatePersistenceListener listener) {
        this.persistenceListener = listener;
    }

    private void notifyPersistenceListener() {
        if (persistenceListener != null) {
            persistenceListener.onStateChanged(this.currentTerm, this.votedFor);
        }
    }

    public synchronized long currentTerm() {
        return currentTerm;
    }

    public synchronized void setCurrentTerm(long newTerm) {
        RaftInvariants.checkTermNeverDecreases(this.currentTerm, newTerm);
        if (newTerm > this.currentTerm) {
            this.currentTerm = newTerm;
            this.votedFor = null;
            notifyPersistenceListener();
        }
    }

    public synchronized Optional<NodeId> votedFor() {
        return Optional.ofNullable(votedFor);
    }

    public synchronized void setVotedFor(NodeId candidateId) {
        this.votedFor = candidateId;
        notifyPersistenceListener();
    }

    public synchronized void updateTermAndVote(long newTerm, NodeId candidateId) {
        RaftInvariants.checkTermNeverDecreases(this.currentTerm, newTerm);
        this.currentTerm = newTerm;
        this.votedFor = candidateId;
        notifyPersistenceListener();
    }
}

