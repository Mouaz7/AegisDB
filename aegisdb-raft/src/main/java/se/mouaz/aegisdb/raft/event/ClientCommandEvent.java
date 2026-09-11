package se.mouaz.aegisdb.raft.event;

import java.util.concurrent.CompletableFuture;

/**
 * Event for client operations executing against the state machine, returning operation results.
 */
public record ClientCommandEvent(
        byte[] command,
        CompletableFuture<byte[]> future
) implements RaftEvent {}
