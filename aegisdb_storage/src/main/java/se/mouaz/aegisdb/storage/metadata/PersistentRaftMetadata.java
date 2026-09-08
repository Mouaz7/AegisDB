package se.mouaz.aegisdb.storage.metadata;

import se.mouaz.aegisdb.common.NodeId;

import java.util.Optional;

/**
 * Immutable value object holding durable consensus metadata (Master Project Plan §7, §8; Ongaro §5.2).
 */
public record PersistentRaftMetadata(long currentTerm, NodeId votedFor) {

    public PersistentRaftMetadata {
        if (currentTerm < 0) {
            throw new IllegalArgumentException("currentTerm cannot be negative: " + currentTerm);
        }
    }

    public Optional<NodeId> optionalVotedFor() {
        return Optional.ofNullable(votedFor);
    }
}
