package se.mouaz.aegisdb.raft.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.raft.log.RaftLog;

import java.util.Objects;

/**
 * Encapsulates the Raft log consistency check according to Ongaro §5.3 (Section 22).
 * Verifies that the follower contains an entry at prevLogIndex whose term matches prevLogTerm.
 */
public class LogConsistencyChecker {
    private static final Logger log = LoggerFactory.getLogger(LogConsistencyChecker.class);

    public record ConsistencyResult(boolean consistent, long conflictHintIndex, String reason) {
        public static ConsistencyResult success(long matchIndex) {
            return new ConsistencyResult(true, matchIndex, "Consistent");
        }

        public static ConsistencyResult failure(long conflictHintIndex, String reason) {
            return new ConsistencyResult(false, conflictHintIndex, reason);
        }
    }

    /**
     * Checks if follower log is consistent with leader's prevLogIndex and prevLogTerm (§5.3).
     */
    public ConsistencyResult check(RaftLog raftLog, AppendEntriesRequest request) {
        Objects.requireNonNull(raftLog, "raftLog cannot be null");
        Objects.requireNonNull(request, "request cannot be null");

        long prevLogIndex = request.prevLogIndex();
        long prevLogTerm = request.prevLogTerm();

        if (prevLogIndex == 0) {
            return ConsistencyResult.success(0);
        }

        if (raftLog.lastLogIndex() < prevLogIndex) {
            log.debug("Consistency check failed: lastLogIndex {} < prevLogIndex {}",
                    raftLog.lastLogIndex(), prevLogIndex);
            return ConsistencyResult.failure(
                    raftLog.lastLogIndex(),
                    "Missing entry at prevLogIndex " + prevLogIndex + ", follower only has " + raftLog.lastLogIndex()
            );
        }

        long actualTerm = raftLog.getTerm(prevLogIndex);
        if (actualTerm != prevLogTerm) {
            log.debug("Consistency check failed: term mismatch at prevLogIndex {}: follower has term {} but leader sent prevLogTerm {}",
                    prevLogIndex, actualTerm, prevLogTerm);
            return ConsistencyResult.failure(
                    Math.max(0, prevLogIndex - 1),
                    "Term mismatch at prevLogIndex " + prevLogIndex + ": follower term " + actualTerm + " != leader " + prevLogTerm
            );
        }

        return ConsistencyResult.success(prevLogIndex);
    }
}
