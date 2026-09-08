package se.mouaz.aegisdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.raft.log.RaftLogRepository;
import se.mouaz.aegisdb.storage.wal.StorageIndex;
import se.mouaz.aegisdb.storage.wal.StorageRecord;
import se.mouaz.aegisdb.storage.wal.WalWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

/**
 * DurableRaftLog provides disk-backed persistence for Raft log entries (Master Project Plan §8, §17; US007).
 * Extends RaftLog for in-memory reads and writes-through to WalWriter on append.
 */
public class DurableRaftLog extends RaftLog implements RaftLogRepository {
    private static final Logger log = LoggerFactory.getLogger(DurableRaftLog.class);

    private final WalWriter walWriter;
    private final StorageIndex storageIndex;

    public DurableRaftLog(WalWriter walWriter, StorageIndex storageIndex) {
        super();
        this.walWriter = Objects.requireNonNull(walWriter, "walWriter cannot be null");
        this.storageIndex = storageIndex != null ? storageIndex : new StorageIndex();
    }

    public DurableRaftLog(WalWriter walWriter, StorageIndex storageIndex, List<RaftLogEntry> initialRecoveredEntries) {
        this(walWriter, storageIndex);
        if (initialRecoveredEntries != null) {
            for (RaftLogEntry entry : initialRecoveredEntries) {
                super.append(entry);
            }
        }
    }

    public StorageIndex storageIndex() {
        return storageIndex;
    }

    public WalWriter walWriter() {
        return walWriter;
    }

    @Override
    public synchronized void append(RaftLogEntry entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Cannot append null entry");
        }

        // 1. Persist to Write-Ahead Log before in-memory state update (Durability rule, §8)
        try {
            StorageRecord record = StorageRecord.createEntry(
                    entry.index(),
                    entry.term(),
                    System.currentTimeMillis(),
                    entry.data()
            );
            StorageIndex.IndexEntry indexEntry = walWriter.append(record);
            storageIndex.put(
                    indexEntry.sequenceNumber(),
                    indexEntry.segmentId(),
                    indexEntry.fileOffset(),
                    indexEntry.recordSize(),
                    indexEntry.term()
            );
        } catch (IOException e) {
            log.error("Failed to append entry {} to WAL", entry.index(), e);
            throw new UncheckedIOException("Failed to write RaftLogEntry to WAL: index=" + entry.index(), e);
        }

        // 2. Append to in-memory log
        super.append(entry);
    }

    @Override
    public synchronized void append(List<RaftLogEntry> newEntries) {
        if (newEntries == null) {
            return;
        }
        for (RaftLogEntry entry : newEntries) {
            append(entry);
        }
    }

    @Override
    public synchronized void truncateFrom(long fromIndex, long commitIndex) {
        // 1. Super class checks safety invariants (committed entries are never truncated)
        super.truncateFrom(fromIndex, commitIndex);

        // 2. Truncate in WAL and index
        try {
            walWriter.truncateFrom(fromIndex, storageIndex);
        } catch (IOException e) {
            log.error("Failed to truncate WAL from index {}", fromIndex, e);
            throw new UncheckedIOException("Failed to truncate WAL from index " + fromIndex, e);
        }
    }

    @Override
    public synchronized void truncateFrom(long fromIndex) {
        truncateFrom(fromIndex, 0L);
    }
}
