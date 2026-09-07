package se.mouaz.aegisdb.raft.election;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.state.RaftState;
import se.mouaz.aegisdb.transport.RaftTransport;

import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * ElectionManager manages candidate elections and vote counting (Section 19, 82).
 */
public class ElectionManager {
    private static final Logger log = LoggerFactory.getLogger(ElectionManager.class);

    private final RaftState state;
    private final RaftTransport transport;
    private final ClusterConfiguration clusterConfig;
    private final ElectionTimer electionTimer;
    private final Consumer<VoteCounter> onLeaderElected;
    private final BiConsumer<NodeId, RequestVoteRequest> requestVoteSender;

    private volatile VoteCounter currentElection;

    public ElectionManager(RaftState state,
                           RaftTransport transport,
                           ClusterConfiguration clusterConfig,
                           ElectionTimer electionTimer,
                           Consumer<VoteCounter> onLeaderElected,
                           BiConsumer<NodeId, RequestVoteRequest> requestVoteSender) {
        this.state = Objects.requireNonNull(state, "state cannot be null");
        this.transport = Objects.requireNonNull(transport, "transport cannot be null");
        this.clusterConfig = Objects.requireNonNull(clusterConfig, "clusterConfig cannot be null");
        this.electionTimer = electionTimer;
        this.onLeaderElected = onLeaderElected;
        this.requestVoteSender = requestVoteSender;
    }

    public synchronized void startElection() {
        if (state.role() == RaftRole.LEADER) {
            log.debug("Node {} is already leader, ignoring election trigger", state.localNodeId());
            return;
        }

        // 1. Transition to Candidate, increase currentTerm, vote for self (§5.2)
        state.becomeCandidate();
        long electionTerm = state.currentTerm();
        NodeId localId = state.localNodeId();

        int clusterSize = Math.max(1, clusterConfig.members().size());
        VoteCounter voteCounter = new VoteCounter(electionTerm, clusterSize);
        voteCounter.recordVote(localId, true); // Vote for self
        this.currentElection = voteCounter;

        log.info("Node {} started election for term {} (cluster size: {}, required majority: {})",
                localId, electionTerm, clusterSize, voteCounter.requiredMajority());

        // 2. Single-node cluster case: wins election immediately! (§5.2)
        if (voteCounter.hasWonElection()) {
            winElection(voteCounter);
            return;
        }

        // 3. Reset election timer for this candidate round (§5.2)
        if (electionTimer != null) {
            electionTimer.reset();
        }

        // 4. Send RequestVote RPCs to all peers in parallel (§5.2)
        RequestVoteRequest request = new RequestVoteRequest(localId, electionTerm, 0, 0);
        Set<NodeId> peers = clusterConfig.members().keySet();
        for (NodeId peer : peers) {
            if (!peer.equals(localId)) {
                if (requestVoteSender != null) {
                    requestVoteSender.accept(peer, request);
                } else {
                    transport.requestVote(peer, request)
                            .whenComplete((resp, ex) -> {
                                if (ex == null && resp != null) {
                                    handleVoteResponse(peer, electionTerm, resp);
                                }
                            });
                }
            }
        }
    }

    public synchronized void handleVoteResponse(NodeId fromPeer, long term, RequestVoteResponse response) {
        long currentTerm = state.currentTerm();

        // 1. Check for higher term in response (§5.1)
        if (response.term() > currentTerm) {
            log.info("Candidate {} discovered higher term {} from peer {}, stepping down to follower",
                    state.localNodeId(), response.term(), fromPeer);
            state.becomeFollower(response.term(), null);
            if (electionTimer != null) {
                electionTimer.reset();
            }
            return;
        }

        // 2. Check if this response is for the active election round
        VoteCounter election = this.currentElection;
        if (election == null || state.role() != RaftRole.CANDIDATE || term != currentTerm) {
            return;
        }

        // 3. Record vote
        if (response.voteGranted()) {
            election.recordVote(fromPeer, true);
            log.info("Node {} received vote from {} for term {} (Total: {}/{})",
                    state.localNodeId(), fromPeer, term, election.grantedCount(), election.requiredMajority());

            if (election.hasWonElection()) {
                winElection(election);
            }
        }
    }

    private void winElection(VoteCounter election) {
        if (state.role() == RaftRole.LEADER) {
            return;
        }

        log.info("Node {} won election for term {} with {} votes! Becoming LEADER.",
                state.localNodeId(), election.electionTerm(), election.grantedCount());
        state.becomeLeader(clusterConfig.members().keySet());

        if (electionTimer != null) {
            electionTimer.cancel();
        }

        if (onLeaderElected != null) {
            onLeaderElected.accept(election);
        }
    }

    public VoteCounter currentElection() {
        return currentElection;
    }
}
