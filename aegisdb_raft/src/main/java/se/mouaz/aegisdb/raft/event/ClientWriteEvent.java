package se.mouaz.aegisdb.raft.event;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Event posted when a client proposes a write command to the Raft leader (Section 83, 107).
 */
public record ClientWriteEvent(
        byte[] command,
        CompletableFuture<Long> future
) implements RaftEvent {
    public ClientWriteEvent {
        Objects.requireNonNull(command, "command cannot be null");
        Objects.requireNonNull(future, "future cannot be null");
    }
}
