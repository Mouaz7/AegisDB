package se.mouaz.aegisdb.raft.election;

import se.mouaz.aegisdb.common.NodeId;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks votes received during an election round (Section 19).
 */
public class VoteCounter {
    private final long electionTerm;
    private final int clusterSize;
    private final Set<NodeId> grantedVotes = ConcurrentHashMap.newKeySet();

    public VoteCounter(long electionTerm, int clusterSize) {
        this.electionTerm = electionTerm;
        this.clusterSize = clusterSize;
    }

    public long electionTerm() {
        return electionTerm;
    }

    public int clusterSize() {
        return clusterSize;
    }

    public int requiredMajority() {
        return (clusterSize / 2) + 1;
    }

    public boolean recordVote(NodeId voterId, boolean granted) {
        if (granted) {
            grantedVotes.add(voterId);
        }
        return hasWonElection();
    }

    public boolean hasWonElection() {
        return grantedVotes.size() >= requiredMajority();
    }

    public int grantedCount() {
        return grantedVotes.size();
    }

    public Set<NodeId> grantedVoters() {
        return Collections.unmodifiableSet(grantedVotes);
    }
}
