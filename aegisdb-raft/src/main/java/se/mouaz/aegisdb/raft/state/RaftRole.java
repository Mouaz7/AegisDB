package se.mouaz.aegisdb.raft.state;

/**
 * Raft node roles (Section 15, 82).
 */
public enum RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
