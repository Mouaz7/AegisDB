package se.mouaz.aegisdb.raft.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.raft.state.VolatileRaftState;

import java.util.List;
import java.util.Objects;

/**
 * Resolves log inconsistencies and appends entries on followers (Section 83; Ongaro §5.3).
 */
public class LogConflictResolver {
    private static final Logger log = LoggerFactory.getLogger(LogConflictResolver.class);

    private final LogConsistencyChecker consistencyChecker;

    public LogConflictResolver(LogConsistencyChecker consistencyChecker) {
        this.consistencyChecker = consistencyChecker != null ? consistencyChecker : new LogConsistencyChecker();
    }

    public LogConflictResolver() {
        this(new LogConsistencyChecker());
    }

    /**
     * Verifies consistency with leader's prevLogIndex/prevLogTerm, repairs any conflicts,
     * appends new entries, and advances commitIndex if needed.
     */
    public AppendEntriesResponse resolveAndAppend(RaftLog raftLog,
                                                  AppendEntriesRequest request,
                                                  VolatileRaftState volatileState,
                                                  long currentTerm) {
        Objects.requireNonNull(raftLog, "raftLog cannot be null");
        Objects.requireNonNull(request, "request cannot be null");
        Objects.requireNonNull(volatileState, "volatileState cannot be null");

        // 1. Consistency check via LogConsistencyChecker (§5.3, Section 22)
        LogConsistencyChecker.ConsistencyResult checkResult = consistencyChecker.check(raftLog, request);
        if (!checkResult.consistent()) {
            return new AppendEntriesResponse(
                    currentTerm,
                    false,
                    checkResult.conflictHintIndex(),
                    checkResult.reason()
            );
        }

        // 2. Process incoming entries
        List<RaftLogEntry> entries = RaftLogEntry.deserializeList(request.entries());
        long prevLogIndex = request.prevLogIndex();
        long insertIndex = prevLogIndex;

        for (RaftLogEntry entry : entries) {
            insertIndex++;
            if (raftLog.lastLogIndex() >= insertIndex) {
                long existingTerm = raftLog.getTerm(insertIndex);
                if (existingTerm != entry.term()) {
                    // Conflict detected: truncate from insertIndex onwards
                    log.info("Repairing log conflict at index {}: replacing term {} with leader term {}",
                            insertIndex, existingTerm, entry.term());
                    raftLog.truncateFrom(insertIndex, volatileState.commitIndex());
                    raftLog.append(entry);
                }
                // If term matches, entry is already identical; continue to next
            } else {
                raftLog.append(entry);
            }
        }

        // 3. Update follower's commitIndex (§5.3: min(leaderCommit, index of last new entry))
        if (request.leaderCommit() > volatileState.commitIndex()) {
            long lastNewEntryIndex = entries.isEmpty() ? prevLogIndex : insertIndex;
            long newCommitIndex = Math.min(request.leaderCommit(), lastNewEntryIndex);
            if (newCommitIndex > volatileState.commitIndex()) {
                log.debug("Advancing follower commitIndex from {} to {}", volatileState.commitIndex(), newCommitIndex);
                volatileState.setCommitIndex(newCommitIndex);
            }
        }

        return AppendEntriesResponse.success(currentTerm, raftLog.lastLogIndex());
    }
}
