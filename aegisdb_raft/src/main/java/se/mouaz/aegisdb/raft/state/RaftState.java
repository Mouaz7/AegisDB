package se.mouaz.aegisdb.raft.state;

import se.mouaz.aegisdb.common.NodeId;

import java.util.Objects;
import java.util.Optional;

/**
 * Composite Raft state (Section 15).
 */
public class RaftState {
    private final NodeId localNodeId;
    private volatile RaftRole role = RaftRole.FOLLOWER;
    private final PersistentRaftState persistentState;
    private final VolatileRaftState volatileState;
    private final LeaderState leaderState;
    private volatile NodeId currentLeader = null;

    public RaftState(NodeId localNodeId, PersistentRaftState persistentState) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId cannot be null");
        this.persistentState = persistentState != null ? persistentState : new PersistentRaftState();
        this.volatileState = new VolatileRaftState();
        this.leaderState = new LeaderState();
    }

    public RaftState(NodeId localNodeId) {
        this(localNodeId, new PersistentRaftState());
    }

    public NodeId localNodeId() {
        return localNodeId;
    }

    public RaftRole role() {
        return role;
    }

    public void setRole(RaftRole role) {
        this.role = Objects.requireNonNull(role, "role cannot be null");
    }

    public PersistentRaftState persistent() {
        return persistentState;
    }

    public VolatileRaftState volatileState() {
        return volatileState;
    }

    public LeaderState leaderState() {
        return leaderState;
    }

    public long currentTerm() {
        return persistentState.currentTerm();
    }

    public Optional<NodeId> currentLeader() {
        return Optional.ofNullable(currentLeader);
    }

    public void setCurrentLeader(NodeId leaderId) {
        this.currentLeader = leaderId;
    }

    public void becomeFollower(long newTerm, NodeId leaderId) {
        persistentState.setCurrentTerm(newTerm);
        this.role = RaftRole.FOLLOWER;
        this.currentLeader = leaderId;
    }

    public void becomeCandidate() {
        long newTerm = persistentState.currentTerm() + 1;
        persistentState.updateTermAndVote(newTerm, localNodeId);
        this.role = RaftRole.CANDIDATE;
        this.currentLeader = null;
    }

    public void becomeLeader(Iterable<NodeId> peers) {
        this.role = RaftRole.LEADER;
        this.currentLeader = localNodeId;
        this.leaderState.initialize(peers, 0L);
        RaftInvariants.recordLeaderElected(persistentState.currentTerm(), localNodeId);
    }
}
