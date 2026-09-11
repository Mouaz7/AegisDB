package se.mouaz.aegisdb.raft.event;

public record HeartbeatTimeoutEvent(
        long termAtScheduling
) implements RaftEvent {
}
