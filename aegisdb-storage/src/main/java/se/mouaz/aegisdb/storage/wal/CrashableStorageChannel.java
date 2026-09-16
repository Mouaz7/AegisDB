package se.mouaz.aegisdb.storage.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Delegating FileChannel wrapper that injects deterministic crash exceptions at exact I/O boundaries.
 *
 * <p><b>Important Architectural Note:</b> This class implements deterministic software fault injection
 * for verifying database crash recovery consistency algorithms. It simulates immediate process halts
 * at precise byte offsets and does not emulate hardware power loss or arbitrary filesystem journal semantics.
 */
public class CrashableStorageChannel extends FileChannel {

    private final FileChannel delegate;
    private final AtomicReference<CrashPoint> activeCrashPoint = new AtomicReference<>(null);
    private volatile boolean metadataInterceptionArmed = false;

    public CrashableStorageChannel(FileChannel delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate FileChannel cannot be null");
    }

    public void arm(CrashPoint crashPoint) {
        this.activeCrashPoint.set(crashPoint);
        this.metadataInterceptionArmed = (crashPoint == CrashPoint.AFTER_FSYNC_BEFORE_METADATA_SAVE);
    }

    public void disarm() {
        this.activeCrashPoint.set(null);
        this.metadataInterceptionArmed = false;
    }

    public CrashPoint activeCrashPoint() {
        return activeCrashPoint.get();
    }

    public boolean isMetadataInterceptionArmed() {
        return metadataInterceptionArmed;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        CrashPoint point = activeCrashPoint.get();
        if (point == null) {
            return delegate.write(src);
        }

        switch (point) {
            case BEFORE_RECORD_HEADER -> {
                disarm();
                throw new StorageCrashException(point, "Simulated crash before writing record framing header");
            }
            case PARTIAL_RECORD_HEADER -> {
                disarm();
                // Framing header is 11 bytes; write only 5 bytes to create torn header
                int writeLen = Math.min(5, src.remaining());
                int oldLimit = src.limit();
                src.limit(src.position() + writeLen);
                delegate.write(src);
                src.limit(oldLimit);
                throw new StorageCrashException(point, "Simulated crash mid-header (wrote " + writeLen + " bytes)");
            }
            case PARTIAL_PAYLOAD -> {
                disarm();
                // Write header (11 bytes) + partial payload
                int totalAvailable = src.remaining();
                int partialLen = Math.min(totalAvailable, 11 + Math.max(1, (totalAvailable - 11) / 2));
                int oldLimit = src.limit();
                src.limit(src.position() + partialLen);
                delegate.write(src);
                src.limit(oldLimit);
                throw new StorageCrashException(point, "Simulated crash mid-payload (wrote " + partialLen + "/" + totalAvailable + " bytes)");
            }
            default -> {
                return delegate.write(src);
            }
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        CrashPoint point = activeCrashPoint.get();
        if (point == CrashPoint.BEFORE_FSYNC) {
            disarm();
            throw new StorageCrashException(point, "Simulated crash immediately before executing fsync");
        }
        if (point == CrashPoint.DURING_FSYNC) {
            disarm();
            throw new StorageCrashException(point, "Simulated I/O failure during fsync call");
        }

        delegate.force(metaData);

        if (point == CrashPoint.AFTER_FSYNC_BEFORE_METADATA_SAVE) {
            disarm();
            throw new StorageCrashException(point, "Simulated crash after fsync completed but before metadata update");
        }
    }

    @Override
    protected void implCloseChannel() throws IOException {
        CrashPoint point = activeCrashPoint.get();
        if (point == CrashPoint.DURING_SEGMENT_ROLLOVER) {
            disarm();
            throw new StorageCrashException(point, "Simulated crash during segment rollover close");
        }
        delegate.close();
    }

    // --- Standard FileChannel Delegation ---

    @Override public int read(ByteBuffer dst) throws IOException { return delegate.read(dst); }
    @Override public long read(ByteBuffer[] dsts, int offset, int length) throws IOException { return delegate.read(dsts, offset, length); }
    @Override public long write(ByteBuffer[] srcs, int offset, int length) throws IOException { return delegate.write(srcs, offset, length); }
    @Override public long position() throws IOException { return delegate.position(); }
    @Override public FileChannel position(long newPosition) throws IOException { return delegate.position(newPosition); }
    @Override public long size() throws IOException { return delegate.size(); }
    @Override public FileChannel truncate(long size) throws IOException { return delegate.truncate(size); }
    @Override public int read(ByteBuffer dst, long position) throws IOException { return delegate.read(dst, position); }
    @Override public int write(ByteBuffer src, long position) throws IOException { return delegate.write(src, position); }
    @Override public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException { return delegate.map(mode, position, size); }
    @Override public FileLock lock(long position, long size, boolean shared) throws IOException { return delegate.lock(position, size, shared); }
    @Override public FileLock tryLock(long position, long size, boolean shared) throws IOException { return delegate.tryLock(position, size, shared); }
    @Override public long transferTo(long position, long count, WritableByteChannel target) throws IOException { return delegate.transferTo(position, count, target); }
    @Override public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException { return delegate.transferFrom(src, position, count); }
}
