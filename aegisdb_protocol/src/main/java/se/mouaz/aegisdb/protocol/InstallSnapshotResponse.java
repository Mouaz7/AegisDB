package se.mouaz.aegisdb.protocol;

/**
 * InstallSnapshot RPC response per Raft Section 7 (Ongaro).
 */
public record InstallSnapshotResponse(
    long term,
    boolean success
) {
    public static InstallSnapshotResponse success(long term) {
        return new InstallSnapshotResponse(term, true);
    }

    public static InstallSnapshotResponse failure(long term) {
        return new InstallSnapshotResponse(term, false);
    }
}
