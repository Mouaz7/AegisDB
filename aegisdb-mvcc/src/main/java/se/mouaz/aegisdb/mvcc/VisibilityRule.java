package se.mouaz.aegisdb.mvcc;

/**
 * Pure, deterministic visibility logic for MVCC snapshot isolation (Master Plan §9).
 */
public final class VisibilityRule {

    private VisibilityRule() {}

    /**
     * Determines whether a given {@link VersionedValue} is visible to the given {@link Snapshot}.
     *
     * Rules enforced:
     * 1. Aborted versions are NEVER visible.
     * 2. If the version was created by the reading transaction itself, it is visible (Read-Your-Own-Writes).
     * 3. Uncommitted versions created by other transactions are NEVER visible (Dirty Read Prevention).
     * 4. If the version committed after the snapshot read timestamp, it is NOT visible (Future Version Invisibility).
     * 5. If the version was in-flight / active at the time the snapshot was established, it is NOT visible.
     * 6. Committed versions created at or before snapshot read timestamp and not active at snapshot start are VISIBLE.
     *
     * @param version  the candidate version in the chain
     * @param snapshot the reader's point-in-time snapshot view
     * @return true if visible, false otherwise
     */
    public static boolean isVisible(VersionedValue version, Snapshot snapshot) {
        if (version == null || snapshot == null) {
            return false;
        }

        // Rule 1: Aborted versions are never visible
        if (version.isAborted()) {
            return false;
        }

        // Rule 2: Own uncommitted writes are visible to the writing transaction
        if (snapshot.readerTxId() > 0 && version.createTxId() == snapshot.readerTxId()) {
            return true;
        }

        // Rule 3: Uncommitted versions from other transactions are invisible
        if (version.isUncommitted()) {
            return false;
        }

        // Rule 4: Was this transaction active when the snapshot was taken?
        if (snapshot.isTxActiveAtSnapshot(version.createTxId())) {
            return false;
        }

        // Rule 5: Future committed versions are invisible
        if (version.commitTimestamp() > snapshot.readTimestamp()) {
            return false;
        }

        // Rule 6: Committed at or before snapshot read timestamp
        return version.isCommitted();
    }
}
