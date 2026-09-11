package se.mouaz.aegisdb.storage.wal;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Configuration options for the Write-Ahead Log (Master Project Plan §8).
 */
public record WalConfig(
        Path walDir,
        long maxSegmentSizeBytes,
        FsyncPolicy fsyncPolicy
) {
    public static final long DEFAULT_MAX_SEGMENT_SIZE_BYTES = 10 * 1024 * 1024L; // 10 MB

    public WalConfig {
        Objects.requireNonNull(walDir, "walDir cannot be null");
        if (maxSegmentSizeBytes <= 0) {
            throw new IllegalArgumentException("maxSegmentSizeBytes must be positive: " + maxSegmentSizeBytes);
        }
        if (fsyncPolicy == null) {
            fsyncPolicy = FsyncPolicy.ALWAYS;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static WalConfig of(Path walDir) {
        return new WalConfig(walDir, DEFAULT_MAX_SEGMENT_SIZE_BYTES, FsyncPolicy.ALWAYS);
    }

    public static class Builder {
        private Path walDir;
        private long maxSegmentSizeBytes = DEFAULT_MAX_SEGMENT_SIZE_BYTES;
        private FsyncPolicy fsyncPolicy = FsyncPolicy.ALWAYS;

        public Builder walDir(Path walDir) {
            this.walDir = walDir;
            return this;
        }

        public Builder maxSegmentSizeBytes(long maxSegmentSizeBytes) {
            this.maxSegmentSizeBytes = maxSegmentSizeBytes;
            return this;
        }

        public Builder fsyncPolicy(FsyncPolicy fsyncPolicy) {
            this.fsyncPolicy = fsyncPolicy;
            return this;
        }

        public WalConfig build() {
            return new WalConfig(walDir, maxSegmentSizeBytes, fsyncPolicy);
        }
    }
}
