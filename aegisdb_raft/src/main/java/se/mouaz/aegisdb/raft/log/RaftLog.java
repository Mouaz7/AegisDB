package se.mouaz.aegisdb.raft.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * In-memory 1-indexed Raft log implementation (Section 18, 83; Ongaro §5.3).
 * Thread-safe for read and append operations.
 */
public class RaftLog implements RaftLogRepository {
    private long snapshotIndex = 0;
    private long snapshotTerm = 0;
    private final List<RaftLogEntry> entries = new ArrayList<>();

    public RaftLog() {
        this(0, 0);
    }

    public RaftLog(long snapshotIndex, long snapshotTerm) {
        this.snapshotIndex = snapshotIndex;
        this.snapshotTerm = snapshotTerm;
        // Index 0 is a sentinel representing empty or snapshot state
        this.entries.add(new RaftLogEntry(snapshotIndex, snapshotTerm, new byte[0]));
    }

    @Override
    public synchronized long snapshotIndex() {
        return snapshotIndex;
    }

    @Override
    public synchronized long snapshotTerm() {
        return snapshotTerm;
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
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        if (index < snapshotIndex || index > lastLogIndex()) {
            return -1;
        }
        int offset = (int) (index - snapshotIndex);
        return entries.get(offset).term();
    }

    public synchronized Optional<RaftLogEntry> getEntry(long index) {
        if (index <= snapshotIndex || index > lastLogIndex()) {
            return Optional.empty();
        }
        int offset = (int) (index - snapshotIndex);
        return Optional.of(entries.get(offset));
    }

    public synchronized boolean matchTerm(long index, long term) {
        if (index == snapshotIndex) {
            return snapshotTerm == term;
        }
        if (index < snapshotIndex || index > lastLogIndex()) {
            return false;
        }
        int offset = (int) (index - snapshotIndex);
        return entries.get(offset).term() == term;
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
        if (startIndex <= snapshotIndex) {
            startIndex = snapshotIndex + 1;
        }
        if (startIndex > lastLogIndex() || maxEntries <= 0) {
            return Collections.emptyList();
        }
        int from = (int) (startIndex - snapshotIndex);
        int to = (int) Math.min((long) entries.size(), (long) from + maxEntries);
        List<RaftLogEntry> slice = new ArrayList<>(entries.subList(from, to));
        return Collections.unmodifiableList(slice);
    }

    public synchronized List<RaftLogEntry> allEntries() {
        if (entries.size() <= 1) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(new ArrayList<>(entries.subList(1, entries.size())));
    }

    /**
     * Truncates log starting at fromIndex (inclusive).
     * Used by LogConflictResolver to delete uncommitted conflicting entries.
     */
    public synchronized void truncateFrom(long fromIndex, long commitIndex) {
        if (fromIndex <= snapshotIndex) {
            throw new IllegalArgumentException("Cannot truncate already compacted entries: fromIndex="
                    + fromIndex + ", snapshotIndex=" + snapshotIndex);
        }
        if (fromIndex <= commitIndex) {
            throw new IllegalStateException("Raft Invariant Violation: Cannot truncate committed entries! fromIndex: "
                    + fromIndex + ", commitIndex: " + commitIndex);
        }
        if (fromIndex > lastLogIndex()) {
            return;
        }
        int offset = (int) (fromIndex - snapshotIndex);
        while (entries.size() > offset) {
            entries.remove(entries.size() - 1);
        }
    }

    /**
     * Truncates log starting at fromIndex without commitIndex check (use with care).
     */
    public synchronized void truncateFrom(long fromIndex) {
        truncateFrom(fromIndex, 0L);
    }

    /**
     * Discards log entries up through newSnapshotIndex.
     * The entry at newSnapshotIndex becomes the new log sentinel.
     */
    @Override
    public synchronized void compactUpTo(long newSnapshotIndex, long newSnapshotTerm) {
        if (newSnapshotIndex <= snapshotIndex) {
            return;
        }
        if (newSnapshotIndex >= lastLogIndex()) {
            entries.clear();
            entries.add(new RaftLogEntry(newSnapshotIndex, newSnapshotTerm, new byte[0]));
        } else {
            int offset = (int) (newSnapshotIndex - snapshotIndex);
            List<RaftLogEntry> remaining = new ArrayList<>(entries.subList(offset + 1, entries.size()));
            entries.clear();
            entries.add(new RaftLogEntry(newSnapshotIndex, newSnapshotTerm, new byte[0]));
            entries.addAll(remaining);
        }
        this.snapshotIndex = newSnapshotIndex;
        this.snapshotTerm = newSnapshotTerm;
    }
}
