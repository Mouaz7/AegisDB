package se.mouaz.aegisdb.storage.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

/**
 * WalManager coordinates segment lifecycle, rollover, appending, and truncation (Master Project Plan §8).
 */
public class WalManager implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(WalManager.class);

    private final WalConfig config;
    private final ConcurrentNavigableMap<Long, WalSegment> segments = new ConcurrentSkipListMap<>();
    private SegmentWriter activeWriter;
    private WalSegment activeSegment;

    public WalManager(WalConfig config) throws IOException {
        this.config = Objects.requireNonNull(config, "config cannot be null");
        initDirectory();
        discoverExistingSegments();
        ensureActiveWriter();
    }

    private void initDirectory() throws IOException {
        Path dir = config.walDir();
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }
    }

    private void discoverExistingSegments() throws IOException {
        try (Stream<Path> stream = Files.list(config.walDir())) {
            stream.filter(WalSegment::isSegmentFile)
                    .forEach(path -> {
                        long segId = WalSegment.parseSegmentId(path);
                        WalSegment segment = new WalSegment(segId, path, false);
                        segments.put(segId, segment);
                    });
        }
    }

    private synchronized void ensureActiveWriter() throws IOException {
        if (segments.isEmpty()) {
            rollToSegment(1L);
        } else {
            long lastSegId = segments.lastKey();
            this.activeSegment = segments.get(lastSegId);
            this.activeWriter = new SegmentWriter(activeSegment, config.fsyncPolicy());
        }
    }

    private synchronized void rollToSegment(long nextSegmentId) throws IOException {
        if (activeWriter != null) {
            activeWriter.close();
            if (activeSegment != null) {
                activeSegment.seal();
            }
        }

        WalSegment newSegment = WalSegment.openOrCreate(config.walDir(), nextSegmentId);
        segments.put(nextSegmentId, newSegment);
        this.activeSegment = newSegment;
        this.activeWriter = new SegmentWriter(newSegment, config.fsyncPolicy());
        log.info("Rolled WAL to active segment: {}", newSegment.path().getFileName());
    }

    /**
     * Appends a record to the active segment. If the active segment exceeds maxSegmentSizeBytes,
     * it rolls over to a new segment first.
     *
     * @return StorageIndex.IndexEntry tracking segmentId, fileOffset, recordSize.
     */
    public synchronized StorageIndex.IndexEntry append(StorageRecord record) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");

        // Check rollover condition
        if (activeWriter.currentPosition() >= config.maxSegmentSizeBytes()) {
            long nextId = activeSegment.segmentId() + 1;
            rollToSegment(nextId);
        }

        long segId = activeSegment.segmentId();
        long fileOffset = activeWriter.append(record);
        int recordSize = record.totalSizeOnDisk();

        return new StorageIndex.IndexEntry(record.sequenceNumber(), segId, fileOffset, recordSize, record.term());
    }

    public synchronized void sync() throws IOException {
        if (activeWriter != null) {
            activeWriter.sync();
        }
    }

    public synchronized WalSegment activeSegment() {
        return activeSegment;
    }

    public synchronized SegmentWriter activeWriter() {
        return activeWriter;
    }

    public List<WalSegment> listSegments() {
        return new ArrayList<>(segments.values());
    }

    public Optional<WalSegment> getSegment(long segmentId) {
        return Optional.ofNullable(segments.get(segmentId));
    }

    public WalConfig config() {
        return config;
    }

    /**
     * Calculates total bytes consumed by all WAL segments on disk.
     */
    public long totalWalSize() {
        long total = 0L;
        for (WalSegment segment : segments.values()) {
            total += segment.size();
        }
        return total;
    }

    /**
     * Truncates log at or after fromSequenceNumber using the in-memory StorageIndex.
     * If the truncation point falls in an earlier segment, newer segments are deleted and the
     * target segment is truncated.
     */
    public synchronized void truncateFrom(long fromSequenceNumber, StorageIndex storageIndex) throws IOException {
        if (fromSequenceNumber <= 0 || storageIndex == null) {
            return;
        }

        Optional<StorageIndex.IndexEntry> entryOpt = storageIndex.get(fromSequenceNumber);
        if (entryOpt.isEmpty()) {
            return;
        }

        StorageIndex.IndexEntry targetEntry = entryOpt.get();
        long targetSegmentId = targetEntry.segmentId();
        long targetOffset = targetEntry.fileOffset();

        // 1. Delete all segments strictly greater than targetSegmentId
        List<Long> toRemove = new ArrayList<>();
        for (Long segId : segments.keySet()) {
            if (segId > targetSegmentId) {
                toRemove.add(segId);
            }
        }

        for (Long segId : toRemove) {
            WalSegment seg = segments.remove(segId);
            if (seg != null) {
                if (seg == activeSegment && activeWriter != null) {
                    activeWriter.close();
                    activeWriter = null;
                }
                Files.deleteIfExists(seg.path());
                log.info("Deleted truncated WAL segment: {}", seg.path().getFileName());
            }
        }

        // 2. Truncate the target segment at targetOffset
        WalSegment targetSegment = segments.get(targetSegmentId);
        if (targetSegment != null) {
            if (activeSegment != targetSegment) {
                if (activeWriter != null) {
                    activeWriter.close();
                }
                this.activeSegment = targetSegment;
                this.activeWriter = new SegmentWriter(targetSegment, config.fsyncPolicy());
            }
            activeWriter.truncate(targetOffset);
            log.info("Truncated segment {} to offset {}", targetSegment.path().getFileName(), targetOffset);
        }

        // 3. Truncate index
        storageIndex.truncateFrom(fromSequenceNumber);
    }

    @Override
    public synchronized void close() throws IOException {
        if (activeWriter != null) {
            activeWriter.close();
            activeWriter = null;
        }
    }
}
