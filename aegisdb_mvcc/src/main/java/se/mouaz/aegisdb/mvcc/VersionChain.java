package se.mouaz.aegisdb.mvcc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lock-free, concurrent singly-linked version chain maintaining chronological versions for a key (Master Plan §9).
 * The newest version is always at the head of the chain.
 */
public class VersionChain {
    private final String key;
    private final AtomicReference<VersionedValue> head = new AtomicReference<>(null);

    public VersionChain(String key) {
        this.key = key;
    }

    public VersionChain(String key, VersionedValue initialVersion) {
        this.key = key;
        this.head.set(initialVersion);
    }

    public String key() {
        return key;
    }

    public VersionedValue head() {
        return head.get();
    }

    /**
     * Atomically prepends a new uncommitted or committed version to the head of the chain.
     */
    public void append(VersionedValue newVersion) {
        while (true) {
            VersionedValue currentHead = head.get();
            VersionedValue updated = new VersionedValue(
                    newVersion.createTxId(),
                    newVersion.commitTimestamp(),
                    newVersion.value(),
                    newVersion.isTombstone(),
                    currentHead
            );
            if (head.compareAndSet(currentHead, updated)) {
                return;
            }
        }
    }

    /**
     * Traverses the version chain from newest to oldest to find the value visible to the given snapshot.
     * Readers traverse without locks, achieving true non-blocking read scalability.
     */
    public Optional<byte[]> findVisible(Snapshot snapshot) {
        VersionedValue node = head.get();
        while (node != null) {
            if (VisibilityRule.isVisible(node, snapshot)) {
                if (node.isTombstone()) {
                    return Optional.empty(); // Deleted in or before this snapshot
                }
                return Optional.of(node.value());
            }
            node = node.next();
        }
        return Optional.empty();
    }

    /**
     * Finds the visible node itself.
     */
    public Optional<VersionedValue> findVisibleNode(Snapshot snapshot) {
        VersionedValue node = head.get();
        while (node != null) {
            if (VisibilityRule.isVisible(node, snapshot)) {
                return Optional.of(node);
            }
            node = node.next();
        }
        return Optional.empty();
    }

    /**
     * Marks all uncommitted versions in this chain created by the given transaction ID as committed.
     */
    public boolean commitVersion(long txId, long commitTimestamp) {
        while (true) {
            VersionedValue currentHead = head.get();
            if (currentHead == null) {
                return false;
            }

            // Rebuild chain from head replacing txId nodes
            boolean modified = false;
            List<VersionedValue> nodes = new ArrayList<>();
            VersionedValue curr = currentHead;
            while (curr != null) {
                if (curr.createTxId() == txId && curr.isUncommitted()) {
                    nodes.add(curr.withCommit(commitTimestamp));
                    modified = true;
                } else {
                    nodes.add(curr);
                }
                curr = curr.next();
            }

            if (!modified) {
                return false;
            }

            // Re-link backwards
            VersionedValue newHead = null;
            for (int i = nodes.size() - 1; i >= 0; i--) {
                VersionedValue v = nodes.get(i);
                newHead = new VersionedValue(v.createTxId(), v.commitTimestamp(), v.value(), v.isTombstone(), newHead);
            }

            if (head.compareAndSet(currentHead, newHead)) {
                return true;
            }
        }
    }

    /**
     * Marks uncommitted versions in this chain created by the given transaction ID as aborted.
     */
    public boolean abortVersion(long txId) {
        while (true) {
            VersionedValue currentHead = head.get();
            if (currentHead == null) {
                return false;
            }

            boolean modified = false;
            List<VersionedValue> nodes = new ArrayList<>();
            VersionedValue curr = currentHead;
            while (curr != null) {
                if (curr.createTxId() == txId && curr.isUncommitted()) {
                    // Abort drops the uncommitted version
                    modified = true;
                } else {
                    nodes.add(curr);
                }
                curr = curr.next();
            }

            if (!modified) {
                return false;
            }

            VersionedValue newHead = null;
            for (int i = nodes.size() - 1; i >= 0; i--) {
                VersionedValue v = nodes.get(i);
                newHead = new VersionedValue(v.createTxId(), v.commitTimestamp(), v.value(), v.isTombstone(), newHead);
            }

            if (head.compareAndSet(currentHead, newHead)) {
                return true;
            }
        }
    }

    /**
     * Prunes versions strictly older than the given watermark timestamp.
     * Retains all versions with commitTimestamp >= watermarkTimestamp,
     * PLUS the single newest committed version with commitTimestamp < watermarkTimestamp
     * (as the baseline for active snapshots at the watermark).
     *
     * @param watermarkTimestamp minimum active snapshot timestamp
     * @return number of versions pruned
     */
    public int pruneOlderThan(long watermarkTimestamp) {
        while (true) {
            VersionedValue currentHead = head.get();
            if (currentHead == null) {
                return 0;
            }

            List<VersionedValue> retained = new ArrayList<>();
            VersionedValue curr = currentHead;
            boolean keptBaseline = false;
            int prunedCount = 0;

            while (curr != null) {
                if (curr.isUncommitted()) {
                    // Always keep uncommitted versions
                    retained.add(curr);
                } else if (curr.commitTimestamp() > watermarkTimestamp) {
                    retained.add(curr);
                } else {
                    // commitTimestamp < watermarkTimestamp
                    if (!keptBaseline && curr.isCommitted()) {
                        retained.add(curr);
                        keptBaseline = true;
                    } else {
                        prunedCount++;
                    }
                }
                curr = curr.next();
            }

            if (prunedCount == 0) {
                return 0;
            }

            VersionedValue newHead = null;
            for (int i = retained.size() - 1; i >= 0; i--) {
                VersionedValue v = retained.get(i);
                newHead = new VersionedValue(v.createTxId(), v.commitTimestamp(), v.value(), v.isTombstone(), newHead);
            }

            if (head.compareAndSet(currentHead, newHead)) {
                return prunedCount;
            }
        }
    }

    /**
     * Checks if this chain has any active (non-tombstone) data.
     */
    public boolean hasActiveCommittedData() {
        VersionedValue h = head.get();
        while (h != null) {
            if (h.isCommitted()) {
                return !h.isTombstone();
            }
            h = h.next();
        }
        return false;
    }

    /**
     * Returns an unmodifiable list of all versions from newest to oldest.
     */
    public List<VersionedValue> allVersions() {
        List<VersionedValue> list = new ArrayList<>();
        VersionedValue curr = head.get();
        while (curr != null) {
            list.add(curr);
            curr = curr.next();
        }
        return Collections.unmodifiableList(list);
    }

    public int versionCount() {
        int count = 0;
        VersionedValue curr = head.get();
        while (curr != null) {
            count++;
            curr = curr.next();
        }
        return count;
    }
}
