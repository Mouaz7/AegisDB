package se.mouaz.aegisdb.storage.wal;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Concrete segment implementation for Write-Ahead Log files (Master Project Plan §8).
 * Files follow the pattern: wal-{segmentId:20d}.seg
 */
public class WalSegment implements StorageSegment {

    public static final String SEGMENT_PREFIX = "wal-";
    public static final String SEGMENT_SUFFIX = ".seg";
    public static final Pattern SEGMENT_PATTERN = Pattern.compile("^wal-(\\d{20})\\.seg$");

    private final long segmentId;
    private final Path path;
    private volatile boolean sealed;
    private volatile long cachedSize;

    public WalSegment(long segmentId, Path path, boolean sealed) {
        this.segmentId = segmentId;
        this.path = Objects.requireNonNull(path, "path cannot be null");
        this.sealed = sealed;
        try {
            this.cachedSize = Files.exists(path) ? Files.size(path) : 0L;
        } catch (IOException e) {
            this.cachedSize = 0L;
        }
    }

    public static String formatSegmentFileName(long segmentId) {
        return String.format("%s%020d%s", SEGMENT_PREFIX, segmentId, SEGMENT_SUFFIX);
    }

    public static boolean isSegmentFile(Path path) {
        return SEGMENT_PATTERN.matcher(path.getFileName().toString()).matches();
    }

    public static long parseSegmentId(Path path) {
        Matcher matcher = SEGMENT_PATTERN.matcher(path.getFileName().toString());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not a valid WAL segment file: " + path.getFileName());
        }
        return Long.parseLong(matcher.group(1));
    }

    public static WalSegment openOrCreate(Path walDir, long segmentId) throws IOException {
        Path segmentPath = walDir.resolve(formatSegmentFileName(segmentId));
        if (!Files.exists(segmentPath)) {
            Files.createFile(segmentPath);
        }
        return new WalSegment(segmentId, segmentPath, false);
    }

    @Override
    public long segmentId() {
        return segmentId;
    }

    @Override
    public Path path() {
        return path;
    }

    @Override
    public long size() {
        try {
            if (Files.exists(path)) {
                this.cachedSize = Files.size(path);
            }
        } catch (IOException ignored) {
            // Keep cached size
        }
        return cachedSize;
    }

    @Override
    public boolean isSealed() {
        return sealed;
    }

    @Override
    public synchronized void seal() throws IOException {
        this.sealed = true;
        this.cachedSize = size();
    }

    public FileChannel openChannel(StandardOpenOption... options) throws IOException {
        return FileChannel.open(path, options);
    }

    @Override
    public void close() {
        // State tracking
    }

    @Override
    public String toString() {
        return "WalSegment{" +
                "id=" + segmentId +
                ", path=" + path.getFileName() +
                ", size=" + size() +
                ", sealed=" + sealed +
                '}';
    }
}
