package se.mouaz.aegisdb.mvcc;

import java.util.Arrays;
import java.util.Objects;

/**
 * Immutable record representing an individual version in an MVCC version chain (Master Plan §9).
 */
public class VersionedValue {
    public static final long UNCOMMITTED = -1L;
    public static final long ABORTED = -2L;

    private final long createTxId;
    private final long commitTimestamp;
    private final byte[] value;
    private final boolean isTombstone;
    private final VersionedValue next;

    public VersionedValue(long createTxId, long commitTimestamp, byte[] value, boolean isTombstone, VersionedValue next) {
        this.createTxId = createTxId;
        this.commitTimestamp = commitTimestamp;
        this.value = value == null ? null : Arrays.copyOf(value, value.length);
        this.isTombstone = isTombstone;
        this.next = next;
    }

    public VersionedValue(long createTxId, long commitTimestamp, byte[] value, boolean isTombstone) {
        this(createTxId, commitTimestamp, value, isTombstone, null);
    }

    public static VersionedValue committed(long txId, long commitTimestamp, byte[] value, VersionedValue next) {
        return new VersionedValue(txId, commitTimestamp, value, false, next);
    }

    public static VersionedValue uncommitted(long txId, byte[] value, VersionedValue next) {
        return new VersionedValue(txId, UNCOMMITTED, value, false, next);
    }

    public static VersionedValue tombstone(long txId, long commitTimestamp, VersionedValue next) {
        return new VersionedValue(txId, commitTimestamp, null, true, next);
    }

    public static VersionedValue uncommittedTombstone(long txId, VersionedValue next) {
        return new VersionedValue(txId, UNCOMMITTED, null, true, next);
    }

    public long createTxId() {
        return createTxId;
    }

    public long commitTimestamp() {
        return commitTimestamp;
    }

    public byte[] value() {
        return value == null ? null : Arrays.copyOf(value, value.length);
    }

    public boolean isTombstone() {
        return isTombstone;
    }

    public VersionedValue next() {
        return next;
    }

    public boolean isCommitted() {
        return commitTimestamp > 0;
    }

    public boolean isUncommitted() {
        return commitTimestamp == UNCOMMITTED;
    }

    public boolean isAborted() {
        return commitTimestamp == ABORTED;
    }

    public VersionedValue withCommit(long commitTimestamp) {
        return new VersionedValue(createTxId, commitTimestamp, value, isTombstone, next);
    }

    public VersionedValue withAbort() {
        return new VersionedValue(createTxId, ABORTED, null, isTombstone, next);
    }

    public VersionedValue withNext(VersionedValue newNext) {
        return new VersionedValue(createTxId, commitTimestamp, value, isTombstone, newNext);
    }

    @Override
    public String toString() {
        return "VersionedValue{" +
                "txId=" + createTxId +
                ", commitTs=" + commitTimestamp +
                ", tombstone=" + isTombstone +
                ", valLen=" + (value == null ? 0 : value.length) +
                ", hasNext=" + (next != null) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionedValue that = (VersionedValue) o;
        return createTxId == that.createTxId &&
                commitTimestamp == that.commitTimestamp &&
                isTombstone == that.isTombstone &&
                Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(createTxId, commitTimestamp, isTombstone);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }
}
