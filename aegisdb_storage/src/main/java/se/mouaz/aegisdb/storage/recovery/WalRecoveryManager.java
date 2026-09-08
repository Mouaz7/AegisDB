package se.mouaz.aegisdb.storage.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.storage.wal.*;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * WalRecoveryManager scans WAL segments, recovers intact records, safely repairs
 * partial-writes (torn tails) at EOF, and reports unrecoverable corruption (Master Project Plan §8, §17).
 */
public class WalRecoveryManager {
    private static final Logger log = LoggerFactory.getLogger(WalRecoveryManager.class);

    private final WalManager walManager;

    public record WalScanResult(
            List<StorageRecord> records,
            StorageIndex storageIndex,
            int recoveredSegmentsCount,
            int tornTailsRepairedCount
    ) {}

    public WalRecoveryManager(WalManager walManager) {
        this.walManager = Objects.requireNonNull(walManager, "walManager cannot be null");
    }

    /**
     * Scans all segments, repairs torn tails if present at EOF, and reconstructs the in-memory index.
     *
     * @return WalScanResult containing valid records and populated StorageIndex.
     * @throws CorruptedWalException if corruption is detected in an intact record.
     */
    public WalScanResult scanAndRecover() throws IOException {
        List<StorageRecord> recoveredRecords = new ArrayList<>();
        StorageIndex storageIndex = new StorageIndex();
        List<WalSegment> segments = walManager.listSegments();

        int segmentsCount = 0;
        int tornTailsRepaired = 0;

        for (WalSegment segment : segments) {
            segmentsCount++;
            boolean segmentHasTornTail = false;
            long tornOffset = -1;

            try (SegmentReader reader = new SegmentReader(segment)) {
                while (true) {
                    SegmentReader.ReadResult result = reader.readNext();

                    if (result instanceof SegmentReader.ReadResult.Success success) {
                        StorageRecord rec = success.record();
                        recoveredRecords.add(rec);
                        storageIndex.put(
                                rec.sequenceNumber(),
                                segment.segmentId(),
                                success.fileOffset(),
                                success.recordSize(),
                                rec.term()
                        );
                    } else if (result instanceof SegmentReader.ReadResult.EndOfFile) {
                        break;
                    } else if (result instanceof SegmentReader.ReadResult.TornTail torn) {
                        segmentHasTornTail = true;
                        tornOffset = torn.fileOffset();
                        log.warn("Detected partial write (torn tail) at offset {} in segment {}: {}. Repairing...",
                                tornOffset, segment.path().getFileName(), torn.reason());
                        break;
                    }
                }
            }

            // Safe partial-write recovery (AC4): truncate torn bytes at EOF
            if (segmentHasTornTail && tornOffset >= 0) {
                truncateSegmentTail(segment, tornOffset);
                tornTailsRepaired++;
            }
        }

        log.info("WAL recovery complete: {} records recovered across {} segments ({} torn tails safely repaired)",
                recoveredRecords.size(), segmentsCount, tornTailsRepaired);

        return new WalScanResult(recoveredRecords, storageIndex, segmentsCount, tornTailsRepaired);
    }

    private void truncateSegmentTail(WalSegment segment, long targetSize) throws IOException {
        try (FileChannel channel = segment.openChannel(StandardOpenOption.WRITE)) {
            channel.truncate(targetSize);
            channel.force(true);
        }
        log.info("Successfully truncated torn tail in segment {} to {} bytes",
                segment.path().getFileName(), targetSize);
    }
}
