package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.ShardLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Cluster-wide Metadata Raft Group and topology authority (§28, Phase 1 Multi-Raft).
 * Commits TopologyCutover as the single, irreversible cluster-wide cutover point,
 * advancing TopologyVersion and atomically publishing new TopologySnapshots.
 */
public class MetadataRaftGroup {

    private static final Logger log = LoggerFactory.getLogger(MetadataRaftGroup.class);

    public record TopologyCutover(
            String splitOperationId,
            long expectedTopologyVersion,
            ShardId parentShardId,
            String parentRaftGroupId,
            KeyRange parentRange,
            ShardEpoch newParentEpoch,
            ShardId childShardId,
            String childRaftGroupId,
            KeyRange childRange,
            ShardEpoch newChildEpoch,
            long snapshotBarrierIndex,
            byte[] snapshotChecksum) {

        public TopologyCutover {
            Objects.requireNonNull(splitOperationId, "splitOperationId cannot be null");
            Objects.requireNonNull(parentShardId, "parentShardId cannot be null");
            Objects.requireNonNull(parentRange, "parentRange cannot be null");
            Objects.requireNonNull(newParentEpoch, "newParentEpoch cannot be null");
            Objects.requireNonNull(childShardId, "childShardId cannot be null");
            Objects.requireNonNull(childRange, "childRange cannot be null");
            Objects.requireNonNull(newChildEpoch, "newChildEpoch cannot be null");
        }
    }

    private final AtomicReference<TopologySnapshot> activeSnapshot;
    private final List<Consumer<TopologySnapshot>> listeners = new CopyOnWriteArrayList<>();

    public MetadataRaftGroup(TopologySnapshot initialSnapshot) {
        this.activeSnapshot = new AtomicReference<>(Objects.requireNonNull(initialSnapshot, "initialSnapshot cannot be null"));
    }

    public MetadataRaftGroup(ShardId initialShardId, int replicaCount) {
        this(TopologySnapshot.initialSingleShard(initialShardId, replicaCount));
    }

    /**
     * Submits and applies a committed TopologyCutover command.
     * This is the single, irreversible cutover authority point for the cluster.
     */
    public synchronized TopologySnapshot commitCutover(TopologyCutover cutover) {
        Objects.requireNonNull(cutover, "cutover cannot be null");
        TopologySnapshot current = activeSnapshot.get();

        long expectedVer = cutover.expectedTopologyVersion();
        if (expectedVer > 0 && current.version().version() != expectedVer) {
            throw new IllegalStateException("Stale topology proposal: expected version " + expectedVer +
                    " but current version is " + current.version().version());
        }

        ShardId parentId = cutover.parentShardId();
        ShardId childId = cutover.childShardId();

        KeyRange parentNewRange = cutover.parentRange();
        KeyRange childNewRange = cutover.childRange();

        ShardEpoch parentNewEpoch = cutover.newParentEpoch();
        ShardEpoch childNewEpoch = cutover.newChildEpoch();

        TopologyVersion nextVersion = current.version().next();

        Map<ShardId, TopologySnapshot.ShardRangeAssignment> assignmentMap = new HashMap<>();
        for (TopologySnapshot.ShardRangeAssignment a : current.rangeAssignments().values()) {
            assignmentMap.put(a.shardId(), a);
        }

        // Update parent
        assignmentMap.put(parentId, new TopologySnapshot.ShardRangeAssignment(
                parentId, parentNewRange, parentNewEpoch, ShardLifecycle.ACTIVE));

        // Add child
        assignmentMap.put(childId, new TopologySnapshot.ShardRangeAssignment(
                childId, childNewRange, childNewEpoch, ShardLifecycle.ACTIVE));

        // Update metadata map
        Map<ShardId, ShardMetadata> metadataMap = new HashMap<>(current.metadataMap());
        ShardMetadata parentOldMeta = metadataMap.get(parentId);
        int replicaCount = parentOldMeta != null ? parentOldMeta.replicaCount() : 3;

        metadataMap.put(parentId, new ShardMetadata(
                parentId, parentNewRange, parentNewEpoch, ShardLifecycle.ACTIVE,
                parentOldMeta != null ? parentOldMeta.createdAt() : java.time.Instant.now(),
                replicaCount, Collections.emptyMap()));

        metadataMap.put(childId, new ShardMetadata(
                childId, childNewRange, childNewEpoch, ShardLifecycle.ACTIVE,
                java.time.Instant.now(), replicaCount, Collections.emptyMap()));

        TopologySnapshot newSnapshot = new TopologySnapshot(nextVersion, assignmentMap.values(), metadataMap.values());
        activeSnapshot.set(newSnapshot);

        log.info("Committed TopologyCutover: opId={}, version={}, parentRange={}, childRange={}",
                cutover.splitOperationId(), nextVersion, parentNewRange, childNewRange);

        for (Consumer<TopologySnapshot> listener : listeners) {
            try {
                listener.accept(newSnapshot);
            } catch (Exception e) {
                log.warn("Failed to notify topology listener", e);
            }
        }

        return newSnapshot;
    }

    public TopologySnapshot currentSnapshot() {
        return activeSnapshot.get();
    }

    public void addListener(Consumer<TopologySnapshot> listener) {
        listeners.add(Objects.requireNonNull(listener, "listener cannot be null"));
    }
}
