package se.mouaz.aegisdb.raft.log;

import java.util.List;
import java.util.Optional;

/**
 * Storage contract for the Raft log (Section 18).
 * Implemented in-memory by RaftLog and prepared for WAL persistence in Sprint 4.
 */
public interface RaftLogRepository {
    long lastLogIndex();
    long lastLogTerm();
    long size();
    boolean isEmpty();
    long getTerm(long index);
    Optional<RaftLogEntry> getEntry(long index);
    List<RaftLogEntry> getEntriesFrom(long startIndex);
    List<RaftLogEntry> getEntriesFrom(long startIndex, int maxEntries);
    List<RaftLogEntry> allEntries();
    boolean matchTerm(long index, long term);
    void append(RaftLogEntry entry);
    void append(List<RaftLogEntry> entries);
    void truncateFrom(long fromIndex, long commitIndex);
    void truncateFrom(long fromIndex);
    default long snapshotIndex() { return 0; }
    default long snapshotTerm() { return 0; }
    default void compactUpTo(long snapshotIndex, long snapshotTerm) {}
}
