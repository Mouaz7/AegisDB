package se.mouaz.aegisdb.raft.state;

import se.mouaz.aegisdb.common.NodeId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime and test verifiers for Raft election invariants (Section 20 & 75).
 */
public final class RaftInvariants {
    private static final Map<Long, NodeId> ELECTED_LEADERS_PER_TERM = new ConcurrentHashMap<>();

    private RaftInvariants() {}

    public static void checkTermNeverDecreases(long oldTerm, long newTerm) {
        if (newTerm < oldTerm) {
            throw new IllegalStateException("Raft Invariant Violation: Term cannot decrease! Old: " + oldTerm + ", New: " + newTerm);
        }
    }

    public static void checkVoteOncePerTerm(long term, NodeId alreadyVotedFor, NodeId candidateId) {
        if (alreadyVotedFor != null && !alreadyVotedFor.equals(candidateId)) {
            throw new IllegalStateException("Raft Invariant Violation: Already voted for " + alreadyVotedFor + " in term " + term + ", cannot vote for " + candidateId);
        }
    }

    public static synchronized void recordLeaderElected(long term, NodeId leaderId) {
        NodeId existing = ELECTED_LEADERS_PER_TERM.putIfAbsent(term, leaderId);
        if (existing != null && !existing.equals(leaderId)) {
            throw new IllegalStateException("Raft Invariant Violation: Split leader! Two leaders in term " + term + ": " + existing + " and " + leaderId);
        }
    }

    public static void clearInvariantTracking() {
        ELECTED_LEADERS_PER_TERM.clear();
    }
}
