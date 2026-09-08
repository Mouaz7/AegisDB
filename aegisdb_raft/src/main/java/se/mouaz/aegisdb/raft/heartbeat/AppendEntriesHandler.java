package se.mouaz.aegisdb.raft.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.raft.election.ElectionTimer;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.replication.LogConflictResolver;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.state.RaftState;

import java.util.Objects;

/**
 * Handles incoming AppendEntries (heartbeats and log replication) according to Raft rules (§5.1, §5.2, §5.3).
 */
public class AppendEntriesHandler {
    private static final Logger log = LoggerFactory.getLogger(AppendEntriesHandler.class);

    private final RaftState state;
    private final ElectionTimer electionTimer;
    private final RaftLog raftLog;
    private final LogConflictResolver conflictResolver;

    public AppendEntriesHandler(RaftState state,
                                ElectionTimer electionTimer,
                                RaftLog raftLog,
                                LogConflictResolver conflictResolver) {
        this.state = Objects.requireNonNull(state, "state cannot be null");
        this.electionTimer = electionTimer;
        this.raftLog = raftLog != null ? raftLog : new RaftLog();
        this.conflictResolver = conflictResolver != null ? conflictResolver : new LogConflictResolver();
    }

    public AppendEntriesHandler(RaftState state, ElectionTimer electionTimer) {
        this(state, electionTimer, new RaftLog(), new LogConflictResolver());
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        long currentTerm = state.currentTerm();
        NodeId localNodeId = state.localNodeId();
        NodeId leaderId = request.leaderId();

        // 1. Rule: Reply false if term < currentTerm (§5.1)
        if (request.term() < currentTerm) {
            log.debug("Node {} rejecting AppendEntries from {} because term {} < currentTerm {}",
                    localNodeId, leaderId, request.term(), currentTerm);
            return AppendEntriesResponse.failure(currentTerm, "Leader term is older than current term");
        }

        // 2. Rule: If term > currentTerm, become follower and update currentTerm (§5.1)
        if (request.term() > currentTerm) {
            log.info("Node {} stepping down to follower: discovered higher term {} from leader {}",
                    localNodeId, request.term(), leaderId);
            state.becomeFollower(request.term(), leaderId);
            currentTerm = request.term();
        } else if (state.role() == RaftRole.CANDIDATE) {
            // While waiting for votes, another server with same or higher term was elected leader (§5.2)
            log.info("Candidate {} stepping down to follower: valid leader {} recognized for term {}",
                    localNodeId, leaderId, currentTerm);
            state.becomeFollower(currentTerm, leaderId);
        } else {
            state.setCurrentLeader(leaderId);
        }

        // 3. Reset election timer because we heard from the valid leader (§5.2)
        if (electionTimer != null) {
            electionTimer.reset();
        }

        // 4. Delegate log consistency check, conflict resolution, and appending to LogConflictResolver (§5.3)
        return conflictResolver.resolveAndAppend(raftLog, request, state.volatileState(), currentTerm);
    }
}
