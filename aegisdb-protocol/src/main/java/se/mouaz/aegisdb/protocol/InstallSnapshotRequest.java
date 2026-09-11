package se.mouaz.aegisdb.protocol;

import se.mouaz.aegisdb.common.NodeId;

import java.util.Arrays;
import java.util.Objects;

/**
 * InstallSnapshot RPC request arguments per Raft Section 7 (Ongaro).
 */
public record InstallSnapshotRequest(
    long term,
    NodeId leaderId,
    long lastIncludedIndex,
    long lastIncludedTerm,
    long offset,
    byte[] data,
    boolean done
) {
    public InstallSnapshotRequest {
        Objects.requireNonNull(leaderId, "leaderId cannot be null");
        data = data == null ? new byte[0] : data.clone();
    }

    @Override
    public byte[] data() {
        return data.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstallSnapshotRequest that)) return false;
        return term == that.term &&
                lastIncludedIndex == that.lastIncludedIndex &&
                lastIncludedTerm == that.lastIncludedTerm &&
                offset == that.offset &&
                done == that.done &&
                leaderId.equals(that.leaderId) &&
                Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, done);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
