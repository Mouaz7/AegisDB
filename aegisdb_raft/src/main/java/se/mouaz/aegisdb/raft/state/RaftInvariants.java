package se.mouaz.aegisdb.raft.state;

import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime and test verifiers for Raft invariants (Section 20 & 75; Ongaro §5.4).
 *
 * Enforces:
 * 1. At most one leader per term
 * 2. Terms never decrease
 * 3. A node votes at most once per term
 * 4. Committed entries are never overwritten (Leader Completeness & State Machine Safety)
 * 5. Committed entries appear in identical order
 */
public final class RaftInvariants {
    private static final Map<Long, NodeId> ELECTED_LEADERS_PER_TERM = new ConcurrentHashMap<>();
    private static final Map<Long, RaftLogEntry> COMMITTED_ENTRIES = new ConcurrentHashMap<>();

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

    /**
     * Invariant: Committed entries are never overwritten (Section 75).
     */
    public static void assertCommittedEntriesNeverOverwritten(long commitIndex, RaftLog log) {
        for (long idx = 1; idx <= commitIndex; idx++) {
            Optional<RaftLogEntry> entryOpt = log.getEntry(idx);
            if (entryOpt.isPresent()) {
                RaftLogEntry entry = entryOpt.get();
                RaftLogEntry existing = COMMITTED_ENTRIES.putIfAbsent(idx, entry);
                if (existing != null && !existing.equals(entry)) {
                    throw new IllegalStateException("Raft Invariant Violation: Committed entry at index " + idx
                            + " was overwritten! Prior committed: " + existing + ", new: " + entry);
                }
            }
        }
    }

    /**
     * Invariant: Committed entries appear in identical order across nodes (Section 75).
     */
    public static void assertIdenticalOrderOfCommittedEntries(RaftLog logA, RaftLog logB, long commonCommitIndex) {
        for (long idx = 1; idx <= commonCommitIndex; idx++) {
            Optional<RaftLogEntry> entryA = logA.getEntry(idx);
            Optional<RaftLogEntry> entryB = logB.getEntry(idx);
            if (entryA.isPresent() && entryB.isPresent()) {
                if (!entryA.get().equals(entryB.get())) {
                    throw new IllegalStateException("Raft Invariant Violation: Divergent committed entry at index " + idx
                            + "! Node A: " + entryA.get() + ", Node B: " + entryB.get());
                }
            }
        }
    }

    public static void clearInvariantTracking() {
        ELECTED_LEADERS_PER_TERM.clear();
        COMMITTED_ENTRIES.clear();
    }
}
