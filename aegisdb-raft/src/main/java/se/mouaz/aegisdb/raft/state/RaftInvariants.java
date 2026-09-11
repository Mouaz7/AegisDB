package se.mouaz.aegisdb.raft.state;

import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Runtime and test verifiers for Raft invariants (Section 20 & 75; Ongaro §5.4).
 * Supports multi-cluster / multi-shard invariant tracking so discrete consensus groups
 * independently verify safety without false-positive collisions.
 *
 * Enforces:
 * 1. At most one leader per term (per consensus group)
 * 2. Terms never decrease
 * 3. A node votes at most once per term
 * 4. Committed entries are never overwritten (Leader Completeness & State Machine Safety)
 * 5. Committed entries appear in identical order
 */
public final class RaftInvariants {
    private static final Map<String, Map<Long, NodeId>> ELECTED_LEADERS_PER_CLUSTER = new ConcurrentHashMap<>();
    private static final Map<String, Map<Long, RaftLogEntry>> COMMITTED_ENTRIES_PER_CLUSTER = new ConcurrentHashMap<>();

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

    public static synchronized void recordLeaderElected(String clusterId, long term, NodeId leaderId) {
        String key = (clusterId != null && !clusterId.isBlank()) ? clusterId : "default";
        Map<Long, NodeId> clusterLeaders = ELECTED_LEADERS_PER_CLUSTER.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        NodeId existing = clusterLeaders.putIfAbsent(term, leaderId);
        if (existing != null && !existing.equals(leaderId)) {
            throw new IllegalStateException("Raft Invariant Violation [Cluster " + key + "]: Split leader! Two leaders in term " + term + ": " + existing + " and " + leaderId);
        }
    }

    public static synchronized void recordLeaderElected(long term, NodeId leaderId) {
        recordLeaderElected("default", term, leaderId);
    }

    /**
     * Invariant: Committed entries are never overwritten (Section 75).
     */
    public static void assertCommittedEntriesNeverOverwritten(String clusterId, long commitIndex, RaftLog log) {
        String key = (clusterId != null && !clusterId.isBlank()) ? clusterId : "default";
        Map<Long, RaftLogEntry> clusterEntries = COMMITTED_ENTRIES_PER_CLUSTER.computeIfAbsent(key, k -> new ConcurrentHashMap<>());
        for (long idx = 1; idx <= commitIndex; idx++) {
            Optional<RaftLogEntry> entryOpt = log.getEntry(idx);
            if (entryOpt.isPresent()) {
                RaftLogEntry entry = entryOpt.get();
                RaftLogEntry existing = clusterEntries.putIfAbsent(idx, entry);
                if (existing != null && !existing.equals(entry)) {
                    throw new IllegalStateException("Raft Invariant Violation [Cluster " + key + "]: Committed entry at index " + idx
                            + " was overwritten! Prior committed: " + existing + ", new: " + entry);
                }
            }
        }
    }

    public static void assertCommittedEntriesNeverOverwritten(long commitIndex, RaftLog log) {
        assertCommittedEntriesNeverOverwritten("default", commitIndex, log);
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
        ELECTED_LEADERS_PER_CLUSTER.clear();
        COMMITTED_ENTRIES_PER_CLUSTER.clear();
    }
}
