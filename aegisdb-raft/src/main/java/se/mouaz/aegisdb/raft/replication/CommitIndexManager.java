package se.mouaz.aegisdb.raft.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.log.RaftLog;

import java.util.Collection;
import java.util.Objects;

/**
 * Calculates whether commitIndex can advance based on follower match indexes (Section 83; Ongaro §5.3, §5.4).
 *
 * Leader commit rule (§5.4.2):
 * If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
 * and log[N].term == currentTerm: set commitIndex = N.
 */
public class CommitIndexManager {
    private static final Logger log = LoggerFactory.getLogger(CommitIndexManager.class);

    /**
     * Computes the new commitIndex based on the current cluster match indexes.
     *
     * @param raftLog the leader's log
     * @param currentTerm the leader's current term
     * @param currentCommitIndex the current commitIndex
     * @param followerStates replication state of all followers
     * @param totalClusterNodes total number of voting nodes in cluster (including leader)
     * @return the advanced commitIndex, or currentCommitIndex if no advance is possible
     */
    public long computeNewCommitIndex(RaftLog raftLog,
                                      long currentTerm,
                                      long currentCommitIndex,
                                      Collection<FollowerReplicationState> followerStates,
                                      int totalClusterNodes) {
        Objects.requireNonNull(raftLog, "raftLog cannot be null");
        Objects.requireNonNull(followerStates, "followerStates cannot be null");

        long lastLogIndex = raftLog.lastLogIndex();
        if (lastLogIndex <= currentCommitIndex) {
            return currentCommitIndex;
        }

        int majorityRequired = (totalClusterNodes / 2) + 1;

        // Search downwards from lastLogIndex to currentCommitIndex + 1
        for (long n = lastLogIndex; n > currentCommitIndex; n--) {
            // Leader always has its own entries replicated
            int replicaCount = 1;

            for (FollowerReplicationState follower : followerStates) {
                if (follower.matchIndex() >= n) {
                    replicaCount++;
                }
            }

            if (replicaCount >= majorityRequired) {
                // Check §5.4.2: Only log entries from current term can be committed directly
                if (raftLog.getTerm(n) == currentTerm) {
                    log.debug("CommitIndex advancing from {} to {} (replicaCount={}/{}, currentTerm={})",
                            currentCommitIndex, n, replicaCount, totalClusterNodes, currentTerm);
                    return n;
                } else {
                    log.debug("Index {} reached majority ({}/{}) but term {} != currentTerm {}, not committing directly",
                            n, replicaCount, totalClusterNodes, raftLog.getTerm(n), currentTerm);
                    // Do not check earlier entries if current term entry hasn't been committed
                    break;
                }
            }
        }

        return currentCommitIndex;
    }
}
