package se.mouaz.aegisdb.raft.event;

public record ElectionTimeoutEvent(
        long termAtScheduling
) implements RaftEvent {
}
