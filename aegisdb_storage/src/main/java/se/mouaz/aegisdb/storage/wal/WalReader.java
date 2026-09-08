package se.mouaz.aegisdb.storage.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Sequential reader across all WAL segments in chronological order (Master Project Plan §8).
 */
public class WalReader implements Closeable {

    private final WalManager walManager;

    public WalReader(WalManager walManager) {
        this.walManager = Objects.requireNonNull(walManager, "walManager cannot be null");
    }

    /**
     * Reads all intact records across all segments currently present in the WAL directory.
     * Stops cleanly if a torn tail is encountered at EOF.
     */
    public List<StorageRecord> readAllRecords() throws IOException {
        List<StorageRecord> records = new ArrayList<>();
        List<WalSegment> segments = walManager.listSegments();

        for (WalSegment segment : segments) {
            try (SegmentReader reader = new SegmentReader(segment)) {
                while (true) {
                    SegmentReader.ReadResult result = reader.readNext();
                    if (result instanceof SegmentReader.ReadResult.Success success) {
                        records.add(success.record());
                    } else if (result instanceof SegmentReader.ReadResult.EndOfFile) {
                        break;
                    } else if (result instanceof SegmentReader.ReadResult.TornTail) {
                        // Torn tail at file end
                        break;
                    }
                }
            }
        }

        return records;
    }

    @Override
    public void close() throws IOException {
        // No persistent resources held
    }
}
