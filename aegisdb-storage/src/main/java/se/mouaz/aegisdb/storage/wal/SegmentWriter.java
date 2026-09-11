package se.mouaz.aegisdb.storage.wal;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * SegmentWriter writes framed StorageRecords to a single WAL segment file (Master Project Plan §8).
 */
public class SegmentWriter implements Closeable {

    private final WalSegment segment;
    private final FileChannel channel;
    private final FsyncPolicy fsyncPolicy;
    private volatile long currentPosition;

    public SegmentWriter(WalSegment segment, FsyncPolicy fsyncPolicy) throws IOException {
        this.segment = Objects.requireNonNull(segment, "segment cannot be null");
        this.fsyncPolicy = fsyncPolicy != null ? fsyncPolicy : FsyncPolicy.ALWAYS;
        this.channel = segment.openChannel(
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
        this.currentPosition = channel.size();
    }

    public WalSegment segment() {
        return segment;
    }

    public long currentPosition() {
        return currentPosition;
    }

    /**
     * Appends a record to the segment and returns the starting offset of the written record.
     */
    public synchronized long append(StorageRecord record) throws IOException {
        Objects.requireNonNull(record, "record cannot be null");
        long recordOffset = currentPosition;
        ByteBuffer buffer = record.serialize();

        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }

        currentPosition += record.totalSizeOnDisk();

        if (fsyncPolicy == FsyncPolicy.ALWAYS) {
            sync();
        }

        return recordOffset;
    }

    /**
     * Flushes channel buffers to disk.
     */
    public synchronized void sync() throws IOException {
        channel.force(true);
    }

    /**
     * Returns the current size of the segment.
     */
    public long size() throws IOException {
        return channel.size();
    }

    /**
     * Truncates the segment to a specified byte size (used during partial-write recovery).
     */
    public synchronized void truncate(long size) throws IOException {
        channel.truncate(size);
        channel.position(size);
        this.currentPosition = size;
        sync();
    }

    @Override
    public synchronized void close() throws IOException {
        try {
            sync();
        } finally {
            channel.close();
        }
    }
}
