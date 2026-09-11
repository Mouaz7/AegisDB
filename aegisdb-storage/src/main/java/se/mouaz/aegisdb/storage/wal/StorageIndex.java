package se.mouaz.aegisdb.storage.wal;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory index for fast O(1) random record lookups by sequence number / log index (Master Project Plan §8).
 */
public class StorageIndex {

    public record IndexEntry(long sequenceNumber, long segmentId, long fileOffset, int recordSize, long term) {}

    private final ConcurrentNavigableMap<Long, IndexEntry> indexMap = new ConcurrentSkipListMap<>();

    public void put(long sequenceNumber, long segmentId, long fileOffset, int recordSize, long term) {
        indexMap.put(sequenceNumber, new IndexEntry(sequenceNumber, segmentId, fileOffset, recordSize, term));
    }

    public Optional<IndexEntry> get(long sequenceNumber) {
        return Optional.ofNullable(indexMap.get(sequenceNumber));
    }

    public boolean contains(long sequenceNumber) {
        return indexMap.containsKey(sequenceNumber);
    }

    public long firstSequenceNumber() {
        return indexMap.isEmpty() ? 0L : indexMap.firstKey();
    }

    public long lastSequenceNumber() {
        return indexMap.isEmpty() ? 0L : indexMap.lastKey();
    }

    public long size() {
        return indexMap.size();
    }

    public boolean isEmpty() {
        return indexMap.isEmpty();
    }

    public Map<Long, IndexEntry> allEntries() {
        return Collections.unmodifiableMap(indexMap);
    }

    /**
     * Truncates all index entries >= fromIndex (used when uncommitted conflicting entries are repaired).
     */
    public void truncateFrom(long fromIndex) {
        if (fromIndex <= 0) {
            return;
        }
        indexMap.tailMap(fromIndex, true).clear();
    }

    /**
     * Purges index entries strictly less than upToSequenceNumber (used after snapshot compaction).
     */
    public void purgeBefore(long upToSequenceNumber) {
        if (upToSequenceNumber <= 0) {
            return;
        }
        indexMap.headMap(upToSequenceNumber, false).clear();
    }

    public void clear() {
        indexMap.clear();
    }
}
