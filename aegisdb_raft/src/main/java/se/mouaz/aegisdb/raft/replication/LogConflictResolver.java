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

        long prevLogIndex = request.prevLogIndex();
        long prevLogTerm = request.prevLogTerm();

        // 1. Reply false if log doesn't contain an entry at prevLogIndex matching prevLogTerm (§5.3)
        if (prevLogIndex > 0) {
            if (raftLog.lastLogIndex() < prevLogIndex) {
                log.debug("Log reject: lastLogIndex {} < prevLogIndex {}", raftLog.lastLogIndex(), prevLogIndex);
                return new AppendEntriesResponse(
                        currentTerm,
                        false,
                        raftLog.lastLogIndex(),
                        "Missing entry at prevLogIndex " + prevLogIndex + ", follower only has " + raftLog.lastLogIndex()
                );
            }

            long actualTermAtPrev = raftLog.getTerm(prevLogIndex);
            if (actualTermAtPrev != prevLogTerm) {
                log.debug("Log reject: term mismatch at prevLogIndex {}: follower has term {} but leader sent prevLogTerm {}",
                        prevLogIndex, actualTermAtPrev, prevLogTerm);
                return new AppendEntriesResponse(
                        currentTerm,
                        false,
                        Math.max(0, prevLogIndex - 1),
                        "Term mismatch at prevLogIndex " + prevLogIndex + ": follower term " + actualTermAtPrev + " != leader " + prevLogTerm
                );
            }
        }

        // 2. Process incoming entries
        List<RaftLogEntry> entries = RaftLogEntry.deserializeList(request.entries());
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

        // 3. Update follower's commitIndex (§5.3)
        if (request.leaderCommit() > volatileState.commitIndex()) {
            long newCommitIndex = Math.min(request.leaderCommit(), raftLog.lastLogIndex());
            log.debug("Advancing follower commitIndex from {} to {}", volatileState.commitIndex(), newCommitIndex);
            volatileState.setCommitIndex(newCommitIndex);
        }

        return AppendEntriesResponse.success(currentTerm, raftLog.lastLogIndex());
    }
}
