package se.mouaz.jdistdb.protocol;

import se.mouaz.jdistdb.common.NodeId;
import java.util.Arrays;
import java.util.Objects;

public record AppendEntriesRequest(
    long term,
    NodeId leaderId,
    long prevLogIndex,
    long prevLogTerm,
    byte[] entries,
    long leaderCommit
) {
    public AppendEntriesRequest {
        Objects.requireNonNull(leaderId, "leaderId cannot be null");
        entries = entries == null ? new byte[0] : entries.clone();
    }

    public static AppendEntriesRequest heartbeat(long term, NodeId leaderId, long prevLogIndex, long prevLogTerm, long leaderCommit) {
        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, new byte[0], leaderCommit);
    }

    public boolean isHeartbeat() {
        return entries.length == 0;
    }

    @Override
    public byte[] entries() {
        return entries.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AppendEntriesRequest that)) return false;
        return term == that.term &&
                prevLogIndex == that.prevLogIndex &&
                prevLogTerm == that.prevLogTerm &&
                leaderCommit == that.leaderCommit &&
                leaderId.equals(that.leaderId) &&
                Arrays.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(term, leaderId, prevLogIndex, prevLogTerm, leaderCommit);
        result = 31 * result + Arrays.hashCode(entries);
        return result;
    }
}
