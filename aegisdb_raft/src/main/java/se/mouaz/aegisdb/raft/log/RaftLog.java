package se.mouaz.aegisdb.raft.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * In-memory 1-indexed Raft log implementation (Section 18, 83; Ongaro §5.3).
 * Thread-safe for read and append operations.
 */
public class RaftLog {
    private final List<RaftLogEntry> entries = new ArrayList<>();

    public RaftLog() {
        // Index 0 is a sentinel representing empty state (term 0)
        entries.add(new RaftLogEntry(0, 0, new byte[0]));
    }

    public synchronized long lastLogIndex() {
        return entries.get(entries.size() - 1).index();
    }

    public synchronized long lastLogTerm() {
        return entries.get(entries.size() - 1).term();
    }

    public synchronized long size() {
        return lastLogIndex();
    }

    public synchronized boolean isEmpty() {
        return lastLogIndex() == 0;
    }

    public synchronized long getTerm(long index) {
        if (index == 0) {
            return 0;
        }
        if (index < 0 || index > lastLogIndex()) {
            return -1;
        }
        return entries.get((int) index).term();
    }

    public synchronized Optional<RaftLogEntry> getEntry(long index) {
        if (index <= 0 || index > lastLogIndex()) {
            return Optional.empty();
        }
        return Optional.of(entries.get((int) index));
    }

    public synchronized boolean matchTerm(long index, long term) {
        if (index == 0) {
            return term == 0;
        }
        if (index < 0 || index > lastLogIndex()) {
            return false;
        }
        return entries.get((int) index).term() == term;
    }

    public synchronized void append(RaftLogEntry entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Cannot append null entry");
        }
        long expectedIndex = lastLogIndex() + 1;
        if (entry.index() != expectedIndex) {
            throw new IllegalArgumentException("Non-contiguous entry append: expected index "
                    + expectedIndex + " but got " + entry.index());
        }
        entries.add(entry);
    }

    public synchronized void append(List<RaftLogEntry> newEntries) {
        if (newEntries == null) {
            return;
        }
        for (RaftLogEntry entry : newEntries) {
            append(entry);
        }
    }

    public synchronized List<RaftLogEntry> getEntriesFrom(long startIndex) {
        return getEntriesFrom(startIndex, Integer.MAX_VALUE);
    }

    public synchronized List<RaftLogEntry> getEntriesFrom(long startIndex, int maxEntries) {
        if (startIndex <= 0) {
            startIndex = 1;
        }
        if (startIndex > lastLogIndex() || maxEntries <= 0) {
            return Collections.emptyList();
        }
        int from = (int) startIndex;
        int to = (int) Math.min(entries.size(), startIndex + maxEntries);
        List<RaftLogEntry> slice = new ArrayList<>(entries.subList(from, to));
        return Collections.unmodifiableList(slice);
    }

    public synchronized List<RaftLogEntry> allEntries() {
        if (isEmpty()) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(entries.subList(1, entries.size())));
    }

    /**
     * Truncates log starting at fromIndex (inclusive).
     * Used by LogConflictResolver to delete uncommitted conflicting entries.
     */
    public synchronized void truncateFrom(long fromIndex, long commitIndex) {
        if (fromIndex <= 0) {
            throw new IllegalArgumentException("Cannot truncate sentinel index: " + fromIndex);
        }
        if (fromIndex <= commitIndex) {
            throw new IllegalStateException("Raft Invariant Violation: Cannot truncate committed entries! fromIndex: "
                    + fromIndex + ", commitIndex: " + commitIndex);
        }
        if (fromIndex > lastLogIndex()) {
            return;
        }
        while (entries.size() > fromIndex) {
            entries.remove(entries.size() - 1);
        }
    }

    /**
     * Truncates log starting at fromIndex without commitIndex check (use with care).
     */
    public synchronized void truncateFrom(long fromIndex) {
        truncateFrom(fromIndex, 0L);
    }
}
