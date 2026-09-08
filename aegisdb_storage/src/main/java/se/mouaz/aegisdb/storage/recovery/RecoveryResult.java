package se.mouaz.aegisdb.storage.recovery;

import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;

import java.util.List;
import java.util.Optional;

/**
 * Result of the crash recovery sequence (Master Project Plan §8).
 */
public record RecoveryResult(
        long recoveredTerm,
        NodeId recoveredVotedFor,
        long lastLogIndex,
        long lastLogTerm,
        List<RaftLogEntry> replayedEntries,
        int repairedTornTailsCount,
        long recoveryDurationMs
) {
    public Optional<NodeId> optionalVotedFor() {
        return Optional.ofNullable(recoveredVotedFor);
    }
}
