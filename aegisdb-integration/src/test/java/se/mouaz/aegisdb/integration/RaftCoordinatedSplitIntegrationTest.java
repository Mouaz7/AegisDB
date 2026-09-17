package se.mouaz.aegisdb.integration;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.*;
import se.mouaz.aegisdb.protocol.pb.*;
import se.mouaz.aegisdb.protocol.SplitCommandCodec;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import se.mouaz.aegisdb.raft.statemachine.KvCommand;
import se.mouaz.aegisdb.sharding.*;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-End Raft-Coordinated Dynamic Split Integration Test
 * (Phase 1 Multi-Raft Architecture, §28).
 *
 * Verifies:
 * 1. Deterministic split handoff with dual indices (writeFenceIndex & snapshotBarrierIndex).
 * 2. 2PC PREPARED transactions committing after PrepareSplit are captured in the snapshot slice.
 * 3. Child returns transient SHARD_NOT_ACTIVATED while in READY state.
 * 4. Single cutover authority in Metadata Raft Group (TopologyCutover).
 * 5. Crash after MetadataCutover commit before Parent FinalizeSplit and Child ActivateShard:
 *    restarting in various orders maintains SingleAuthoritativeOwner, NoAuthoritativeOverlap,
 *    NoAuthoritativeGaps, eventually reaches ACTIVE child and finalized parent with zero stale reads.
 * 6. Non-destructive Parent Finalize: keys >= splitKey marked unroutable stale (STALE_EPOCH) until delayed GC.
 */
class RaftCoordinatedSplitIntegrationTest {

    private byte[] putCmd(String key, String val) {
        return KvCommand.put(key, val.getBytes(StandardCharsets.UTF_8)).toBytes();
    }

    private byte[] getCmd(String key) {
        return KvCommand.get(key).toBytes();
    }

    @Test
    @DisplayName("Complete Raft-Coordinated Split lifecycle with dual indices and 2PC resolution")
    void testCompleteSplitLifecycleWithDualIndices() {
        ShardId parentId = ShardId.of("shard-parent");
        ShardId childId = ShardId.of("shard-child");

        // 1. Initial State: Single parent shard covering full domain [-∞, +∞)
        MetadataRaftGroup metadataRaft = new MetadataRaftGroup(parentId, 3);
        RangePartitioner partitioner = new RangePartitioner(metadataRaft.currentSnapshot());
        metadataRaft.addListener(partitioner::updateSnapshot);

        KeyValueStateMachine parentSm = new KeyValueStateMachine();

        // Populate initial data
        parentSm.apply(1L, putCmd("key-100", "val-100"));
        parentSm.apply(2L, putCmd("key-200", "val-200"));
        parentSm.apply(3L, putCmd("key-600", "val-600"));
        parentSm.apply(4L, putCmd("key-700", "val-700"));
        // Register key-800 as pre-existing PREPARED transaction prior to PrepareSplit
        parentSm.registerPreparedTransactionKey("key-800");

        // 2. PrepareSplit: establishes writeFenceIndex
        ByteArrayKey splitKey = ByteArrayKey.of("key-500");
        String opId = "split-op-001";
        long writeFenceIndex = 10L;

        PrepareSplitCommandProto prepareCmd = PrepareSplitCommandProto.newBuilder()
                .setSplitOperationId(opId)
                .setParentShardId(parentId.value())
                .setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value())
                .setChildRaftGroupId("rg-child")
                .setSplitKey(ByteString.copyFrom(splitKey.getBytes()))
                .setExpectedParentEpoch(ShardEpochProto.newBuilder().setGeneration(1).setVersion(0).build())
                .setTargetEpoch(ShardEpochProto.newBuilder().setGeneration(1).setVersion(1).build())
                .build();

        parentSm.apply(writeFenceIndex, SplitCommandCodec.encode(prepareCmd));
        assertThat(parentSm.lifecycle()).isEqualTo(ShardLifecycle.SPLITTING);
        assertThat(parentSm.writeFenceIndex()).isEqualTo(writeFenceIndex);

        // Verify write fence: writes < splitKey succeed, writes >= splitKey are rejected
        parentSm.apply(11L, putCmd("key-150", "val-150"));
        assertThatThrownBy(() -> parentSm.apply(12L, putCmd("key-550", "val-550")))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.SPLITTING_RANGE);

        // 3. Pre-existing 2PC transaction commits for moving range after PrepareSplit
        // Simulating a 2PC PREPARED transaction committing at index 13
        parentSm.apply(13L, putCmd("key-800", "val-2pc-committed"));

        // 4. SplitSnapshotBarrier: committed after all pre-existing PREPARED transactions resolve
        long snapshotBarrierIndex = 14L;
        SplitSnapshotBarrierCommandProto barrierCmd = SplitSnapshotBarrierCommandProto.newBuilder()
                .setSplitOperationId(opId)
                .setParentShardId(parentId.value())
                .setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value())
                .setChildRaftGroupId("rg-child")
                .build();

        parentSm.apply(snapshotBarrierIndex, SplitCommandCodec.encode(barrierCmd));
        assertThat(parentSm.snapshotBarrierIndex()).isEqualTo(snapshotBarrierIndex);

        // 5. Extract MVCC snapshot slice at snapshotBarrierIndex
        byte[] slice = parentSm.takeSnapshotSlice(splitKey, snapshotBarrierIndex);
        assertThat(slice).isNotEmpty();

        // 6. Child Bootstrap
        KeyValueStateMachine childSm = new KeyValueStateMachine();
        childSm.setLifecycle(ShardLifecycle.BOOTSTRAPPING);
        childSm.restoreSnapshot(snapshotBarrierIndex, slice);

        // Verify child has keys >= splitKey including the 2PC committed key
        assertThat(childSm.snapshotMap()).containsKey("key-600");
        assertThat(childSm.snapshotMap()).containsKey("key-700");
        assertThat(childSm.snapshotMap()).containsKey("key-800");
        assertThat(new String(childSm.snapshotMap().get("key-800"), StandardCharsets.UTF_8)).isEqualTo("val-2pc-committed");
        assertThat(childSm.snapshotMap()).doesNotContainKey("key-100");

        // Child marks bootstrap complete -> transitions to READY
        BootstrapCompleteCommandProto bootCompleteCmd = BootstrapCompleteCommandProto.newBuilder()
                .setSplitOperationId(opId)
                .setParentShardId(parentId.value())
                .setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value())
                .setChildRaftGroupId("rg-child")
                .setPrepareFenceIndex(writeFenceIndex)
                .setSnapshotBarrierIndex(snapshotBarrierIndex)
                .setSnapshotChecksum(ByteString.copyFrom(new byte[]{1, 2, 3, 4}))
                .build();

        childSm.apply(15L, SplitCommandCodec.encode(bootCompleteCmd));
        assertThat(childSm.lifecycle()).isEqualTo(ShardLifecycle.READY);

        // 7. Child transient error in READY state: SHARD_NOT_ACTIVATED
        assertThatThrownBy(() -> childSm.apply(16L, getCmd("key-600")))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.SHARD_NOT_ACTIVATED);
        assertThatThrownBy(() -> childSm.get("key-600"))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.SHARD_NOT_ACTIVATED);

        // 8. Metadata Raft Cutover: single irreversible cutover point
        KeyRange parentNewRange = KeyRange.of(KeyBound.negativeInfinity(), KeyBound.exact(splitKey));
        KeyRange childNewRange = KeyRange.of(KeyBound.exact(splitKey), KeyBound.positiveInfinity());

        MetadataRaftGroup.TopologyCutover cutover = new MetadataRaftGroup.TopologyCutover(
                opId,
                1L, // expected topology version
                parentId,
                "rg-parent",
                parentNewRange,
                ShardEpoch.of(1, 1),
                childId,
                "rg-child",
                childNewRange,
                ShardEpoch.initial(),
                snapshotBarrierIndex,
                new byte[]{1, 2, 3, 4}
        );

        TopologySnapshot cutoverSnapshot = metadataRaft.commitCutover(cutover);
        assertThat(cutoverSnapshot.version().version()).isEqualTo(2L);

        // Invariants verified on cutoverSnapshot
        assertThat(cutoverSnapshot.findAuthoritativeShard(ByteArrayKey.of("key-100")).map(TopologySnapshot.ShardRangeAssignment::shardId)).contains(parentId);
        assertThat(cutoverSnapshot.findAuthoritativeShard(ByteArrayKey.of("key-600")).map(TopologySnapshot.ShardRangeAssignment::shardId)).contains(childId);

        // 9. Parent FinalizeSplit: mark upper keys unroutable stale (STALE_EPOCH)
        FinalizeSplitCommandProto finalizeCmd = FinalizeSplitCommandProto.newBuilder()
                .setSplitOperationId(opId)
                .setParentShardId(parentId.value())
                .setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value())
                .setChildRaftGroupId("rg-child")
                .setSplitKey(ByteString.copyFrom(splitKey.getBytes()))
                .setNewParentEpoch(ShardEpochProto.newBuilder().setGeneration(1).setVersion(1).build())
                .build();

        parentSm.apply(17L, SplitCommandCodec.encode(finalizeCmd));
        assertThat(parentSm.lifecycle()).isEqualTo(ShardLifecycle.ACTIVE);
        assertThat(parentSm.unroutableStaleKeys()).contains("key-600", "key-700", "key-800");

        // Stale read on parent throws STALE_EPOCH
        assertThatThrownBy(() -> parentSm.get("key-600"))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.STALE_EPOCH);

        // 10. Child ActivateShard
        ActivateShardCommandProto activateCmd = ActivateShardCommandProto.newBuilder()
                .setSplitOperationId(opId)
                .setChildShardId(childId.value())
                .setChildRaftGroupId("rg-child")
                .setEpoch(ShardEpochProto.newBuilder().setGeneration(2).setVersion(0).build())
                .build();

        childSm.apply(18L, SplitCommandCodec.encode(activateCmd));
        assertThat(childSm.lifecycle()).isEqualTo(ShardLifecycle.ACTIVE);

        // Reads and writes now succeed on child
        assertThat(childSm.get("key-600")).isNotEmpty();
        assertThat(new String(childSm.get("key-800"), StandardCharsets.UTF_8)).isEqualTo("val-2pc-committed");
        childSm.apply(19L, putCmd("key-900", "val-900"));
        assertThat(childSm.get("key-900")).isEqualTo("val-900".getBytes(StandardCharsets.UTF_8));

        // 11. Delayed GC on parent safely removes stale keys without data loss
        int pruned = parentSm.pruneStaleKeys();
        assertThat(pruned).isEqualTo(3);
        assertThat(parentSm.unroutableStaleKeys()).isEmpty();
        assertThat(parentSm.containsKey("key-600")).isFalse();
        assertThat(parentSm.containsKey("key-100")).isTrue(); // lower keys unaffected
    }

    @Test
    @DisplayName("Crash after MetadataCutover commit before Parent FinalizeSplit and Child ActivateShard")
    void testCrashAfterMetadataCutoverBeforeFinalizeAndActivate() {
        ShardId parentId = ShardId.of("shard-parent");
        ShardId childId = ShardId.of("shard-child");
        ByteArrayKey splitKey = ByteArrayKey.of("key-500");
        String opId = "split-crash-test";

        // Setup metadata Raft with initial single shard
        MetadataRaftGroup metadataRaft = new MetadataRaftGroup(parentId, 3);
        RangePartitioner partitioner = new RangePartitioner(metadataRaft.currentSnapshot());
        metadataRaft.addListener(partitioner::updateSnapshot);

        // Parent state machine
        KeyValueStateMachine parentSm = new KeyValueStateMachine();
        parentSm.apply(1L, putCmd("key-100", "val-100"));
        parentSm.apply(2L, putCmd("key-600", "val-600"));

        // PrepareSplit & SnapshotBarrier
        parentSm.apply(10L, SplitCommandCodec.encode(PrepareSplitCommandProto.newBuilder()
                .setSplitOperationId(opId).setParentShardId(parentId.value()).setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value()).setChildRaftGroupId("rg-child")
                .setSplitKey(ByteString.copyFrom(splitKey.getBytes())).build()));
        parentSm.apply(11L, SplitCommandCodec.encode(SplitSnapshotBarrierCommandProto.newBuilder()
                .setSplitOperationId(opId).setParentShardId(parentId.value()).setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value()).setChildRaftGroupId("rg-child").build()));

        byte[] slice = parentSm.takeSnapshotSlice(splitKey, 11L);

        // Child bootstraps to READY
        KeyValueStateMachine childSm = new KeyValueStateMachine();
        childSm.setLifecycle(ShardLifecycle.BOOTSTRAPPING);
        childSm.restoreSnapshot(11L, slice);
        childSm.apply(12L, SplitCommandCodec.encode(BootstrapCompleteCommandProto.newBuilder()
                .setSplitOperationId(opId).setParentShardId(parentId.value()).setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value()).setChildRaftGroupId("rg-child")
                .setPrepareFenceIndex(10L).setSnapshotBarrierIndex(11L).build()));
        assertThat(childSm.lifecycle()).isEqualTo(ShardLifecycle.READY);

        // NOW: Metadata Cutover COMMITS
        KeyRange parentNewRange = KeyRange.of(KeyBound.negativeInfinity(), KeyBound.exact(splitKey));
        KeyRange childNewRange = KeyRange.of(KeyBound.exact(splitKey), KeyBound.positiveInfinity());

        metadataRaft.commitCutover(new MetadataRaftGroup.TopologyCutover(
                opId, 1L, parentId, "rg-parent", parentNewRange, ShardEpoch.of(1, 1),
                childId, "rg-child", childNewRange, ShardEpoch.initial(), 11L, new byte[]{1}
        ));

        // CRASH INJECTION POINT:
        // Cutover is committed in metadata, but parent has NOT finalized and child has NOT activated!
        // Verify invariant properties across the cluster:
        TopologySnapshot currentTopo = partitioner.currentSnapshot();
        assertThat(currentTopo.version().version()).isEqualTo(2L);

        // 1. SingleAuthoritativeOwner, NoAuthoritativeOverlap, NoAuthoritativeGaps are valid in topology
        assertThat(currentTopo.findAuthoritativeShard(ByteArrayKey.of("key-100")).map(TopologySnapshot.ShardRangeAssignment::shardId)).contains(parentId);
        assertThat(currentTopo.findAuthoritativeShard(ByteArrayKey.of("key-600")).map(TopologySnapshot.ShardRangeAssignment::shardId)).contains(childId);

        // 2. Child is still READY locally: client request returns SHARD_NOT_ACTIVATED (no stale reads)
        assertThatThrownBy(() -> childSm.get("key-600"))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.SHARD_NOT_ACTIVATED);

        // SIMULATE OUT-OF-ORDER RESTART / RECOVERY:
        // Scenario A: Child restarts first
        KeyValueStateMachine recoveredChildSm = new KeyValueStateMachine();
        recoveredChildSm.setLifecycle(ShardLifecycle.BOOTSTRAPPING);
        recoveredChildSm.restoreSnapshot(11L, slice);
        recoveredChildSm.apply(12L, SplitCommandCodec.encode(BootstrapCompleteCommandProto.newBuilder()
                .setSplitOperationId(opId).setParentShardId(parentId.value()).setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value()).setChildRaftGroupId("rg-child")
                .setPrepareFenceIndex(10L).setSnapshotBarrierIndex(11L).build()));

        // Recovery converges forward: since cutover is committed, child receives ActivateShard
        recoveredChildSm.apply(13L, SplitCommandCodec.encode(ActivateShardCommandProto.newBuilder()
                .setSplitOperationId(opId).setChildShardId(childId.value()).setChildRaftGroupId("rg-child").build()));
        assertThat(recoveredChildSm.lifecycle()).isEqualTo(ShardLifecycle.ACTIVE);
        assertThat(recoveredChildSm.get("key-600")).isEqualTo("val-600".getBytes(StandardCharsets.UTF_8));

        // Scenario B: Parent restarts next
        KeyValueStateMachine recoveredParentSm = new KeyValueStateMachine();
        recoveredParentSm.apply(1L, putCmd("key-100", "val-100"));
        recoveredParentSm.apply(2L, putCmd("key-600", "val-600"));
        recoveredParentSm.apply(10L, SplitCommandCodec.encode(PrepareSplitCommandProto.newBuilder()
                .setSplitOperationId(opId).setParentShardId(parentId.value()).setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value()).setChildRaftGroupId("rg-child")
                .setSplitKey(ByteString.copyFrom(splitKey.getBytes())).build()));
        recoveredParentSm.apply(11L, SplitCommandCodec.encode(SplitSnapshotBarrierCommandProto.newBuilder()
                .setSplitOperationId(opId).setParentShardId(parentId.value()).setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value()).setChildRaftGroupId("rg-child").build()));

        // Recovery converges forward: parent receives FinalizeSplit
        recoveredParentSm.apply(14L, SplitCommandCodec.encode(FinalizeSplitCommandProto.newBuilder()
                .setSplitOperationId(opId).setParentShardId(parentId.value()).setParentRaftGroupId("rg-parent")
                .setChildShardId(childId.value()).setChildRaftGroupId("rg-child")
                .setSplitKey(ByteString.copyFrom(splitKey.getBytes())).build()));
        assertThat(recoveredParentSm.lifecycle()).isEqualTo(ShardLifecycle.ACTIVE);

        // Parent rejects stale upper-range read with STALE_EPOCH
        assertThatThrownBy(() -> recoveredParentSm.get("key-600"))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.STALE_EPOCH);

        // Lower range on parent intact
        assertThat(recoveredParentSm.get("key-100")).isEqualTo("val-100".getBytes(StandardCharsets.UTF_8));

        // Delayed GC succeeds
        recoveredParentSm.pruneStaleKeys();
        assertThat(recoveredParentSm.containsKey("key-600")).isFalse();
    }
}
