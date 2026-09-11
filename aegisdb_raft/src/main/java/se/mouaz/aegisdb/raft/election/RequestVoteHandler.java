package se.mouaz.aegisdb.raft.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftState;

import java.util.Objects;
import java.util.Optional;

/**
 * Handles incoming RequestVote requests according to Raft consensus rules (Section 19).
 */
public class RequestVoteHandler {
    private static final Logger log = LoggerFactory.getLogger(RequestVoteHandler.class);

    private final RaftState state;
    private final ElectionTimer electionTimer;
    private final se.mouaz.aegisdb.raft.log.RaftLog raftLog;

    public RequestVoteHandler(RaftState state, ElectionTimer electionTimer, se.mouaz.aegisdb.raft.log.RaftLog raftLog) {
        this.state = Objects.requireNonNull(state, "state cannot be null");
        this.electionTimer = electionTimer;
        this.raftLog = Objects.requireNonNull(raftLog, "raftLog cannot be null");
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        long currentTerm = state.currentTerm();
        NodeId localNodeId = state.localNodeId();
        NodeId candidateId = request.candidateId();

        // 1. Rule: Reply false if term < currentTerm (§5.1)
        if (request.term() < currentTerm) {
            log.debug("Node {} rejecting vote for {} because request term {} < currentTerm {}",
                    localNodeId, candidateId, request.term(), currentTerm);
            return RequestVoteResponse.rejected(currentTerm, "Candidate term is older than current term");
        }

        // 2. Rule: If term > currentTerm, become follower and update term (§5.1)
        if (request.term() > currentTerm) {
            log.info("Node {} stepping down to follower: higher term {} discovered from candidate {}",
                    localNodeId, request.term(), candidateId);
            state.becomeFollower(request.term(), null);
            currentTerm = request.term();
        }

        // 3. Rule: Check if we haven't voted yet, or already voted for this candidate in this term (§5.2, §5.4)
        Optional<NodeId> votedFor = state.persistent().votedFor();
        boolean canVoteForCandidate = votedFor.isEmpty() || votedFor.get().equals(candidateId);

        if (canVoteForCandidate) {
            long localLastLogTerm = raftLog.lastLogTerm();
            long localLastLogIndex = raftLog.lastLogIndex();
            long candidateLastLogTerm = request.lastLogTerm();
            long candidateLastLogIndex = request.lastLogIndex();

            boolean candidateLogIsUpToDate =
                    candidateLastLogTerm > localLastLogTerm
                    || (candidateLastLogTerm == localLastLogTerm
                        && candidateLastLogIndex >= localLastLogIndex);

            if (candidateLogIsUpToDate) {
                RaftInvariants.checkVoteOncePerTerm(currentTerm, votedFor.orElse(null), candidateId);
                state.persistent().setVotedFor(candidateId);
                if (electionTimer != null) {
                    electionTimer.reset();
                }
                log.info("Node {} granted vote to {} for term {}", localNodeId, candidateId, currentTerm);
                return RequestVoteResponse.granted(currentTerm);
            } else {
                return RequestVoteResponse.rejected(currentTerm, "Candidate log is not up-to-date");
            }
        }

        log.debug("Node {} denying vote to {} because already voted for {} in term {}",
                localNodeId, candidateId, votedFor.get(), currentTerm);
        return RequestVoteResponse.rejected(currentTerm, "Already voted for another candidate in term " + currentTerm);
    }
}
