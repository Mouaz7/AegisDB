package se.mouaz.aegisdb.raft.statemachine;

import se.mouaz.aegisdb.common.ByteArrayKey;
import se.mouaz.aegisdb.common.DatabaseException;
import se.mouaz.aegisdb.common.ErrorCode;
import se.mouaz.aegisdb.common.ShardLifecycle;
import se.mouaz.aegisdb.protocol.SplitCommandCodec;
import se.mouaz.aegisdb.protocol.pb.*;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KeyValueStateMachineSplitTest {

    private KeyValueStateMachine stateMachine;

    @BeforeEach
    void setUp() {
        stateMachine = new KeyValueStateMachine();
    }

    @Test
    void testPrepareSplitSetsWriteFenceAndRejectsUpperRangeWrites() {
        for (int i = 1; i <= 10; i++) {
            String key = String.format("key-%03d", i * 10);
            KvCommand cmd = KvCommand.put(key, ("val-" + i).getBytes(StandardCharsets.UTF_8));
            stateMachine.apply((long) i, cmd.toBytes());
        }

        assertThat(stateMachine.size()).isEqualTo(10);
        assertThat(stateMachine.lifecycle()).isEqualTo(ShardLifecycle.ACTIVE);

        // Prepare split at key-050
        PrepareSplitCommandProto prep = PrepareSplitCommandProto.newBuilder()
                .setSplitOperationId("split-op-1")
                .setParentShardId("shard-0")
                .setParentRaftGroupId("group-shard-0")
                .setChildShardId("shard-1")
                .setChildRaftGroupId("group-shard-1")
                .setSplitKey(ByteString.copyFrom("key-050".getBytes(StandardCharsets.UTF_8)))
                .build();

        stateMachine.apply(11L, SplitCommandCodec.encode(prep));

        assertThat(stateMachine.lifecycle()).isEqualTo(ShardLifecycle.SPLITTING);
        assertThat(stateMachine.writeFenceIndex()).isEqualTo(11L);
        assertThat(stateMachine.splitKey()).isEqualTo(ByteArrayKey.of("key-050"));

        // Write below splitKey must succeed
        KvCommand writeBelow = KvCommand.put("key-049", "lower".getBytes(StandardCharsets.UTF_8));
        stateMachine.apply(12L, writeBelow.toBytes());
        assertThat(new String(stateMachine.get("key-049"), StandardCharsets.UTF_8)).isEqualTo("lower");

        // Write at or above splitKey must be rejected with SPLITTING_RANGE
        KvCommand writeAt = KvCommand.put("key-050", "boundary".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> stateMachine.apply(13L, writeAt.toBytes()))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.SPLITTING_RANGE);

        KvCommand writeAbove = KvCommand.put("key-051", "upper".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> stateMachine.apply(14L, writeAbove.toBytes()))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.SPLITTING_RANGE);
    }

    @Test
    void testSnapshotBarrierAndSliceHandoff() {
        for (int i = 1; i <= 10; i++) {
            String key = String.format("key-%03d", i * 10);
            stateMachine.apply((long) i, KvCommand.put(key, ("val-" + i).getBytes(StandardCharsets.UTF_8)).toBytes());
        }

        PrepareSplitCommandProto prep = PrepareSplitCommandProto.newBuilder()
                .setSplitOperationId("split-op-2")
                .setParentShardId("shard-0")
                .setChildShardId("shard-1")
                .setSplitKey(ByteString.copyFrom("key-050".getBytes(StandardCharsets.UTF_8)))
                .build();
        stateMachine.apply(11L, SplitCommandCodec.encode(prep));

        // Commit snapshot barrier
        SplitSnapshotBarrierCommandProto barrier = SplitSnapshotBarrierCommandProto.newBuilder()
                .setSplitOperationId("split-op-2")
                .setParentShardId("shard-0")
                .setChildShardId("shard-1")
                .build();
        stateMachine.apply(12L, SplitCommandCodec.encode(barrier));
        assertThat(stateMachine.snapshotBarrierIndex()).isEqualTo(12L);

        // Take snapshot slice
        byte[] sliceData = stateMachine.takeSnapshotSlice(ByteArrayKey.of("key-050"), 12L);
        assertThat(sliceData).isNotEmpty();
        byte[] checksum = KeyValueStateMachine.computeChecksum(sliceData);
        assertThat(checksum).hasSize(32); // SHA-256

        // Restore into fresh child state machine
        KeyValueStateMachine childSm = new KeyValueStateMachine();
        childSm.restoreSnapshot(12L, sliceData);

        // Child must only have keys >= key-050 (key-050, 060, 070, 080, 090, 100 = 6 entries)
        assertThat(childSm.size()).isEqualTo(6);
        assertThat(childSm.containsKey("key-040")).isFalse();
        assertThat(childSm.containsKey("key-050")).isTrue();
        assertThat(childSm.containsKey("key-100")).isTrue();
    }

    @Test
    void testFinalizeSplitAndDelayedGarbageCollection() {
        for (int i = 1; i <= 10; i++) {
            String key = String.format("key-%03d", i * 10);
            stateMachine.apply((long) i, KvCommand.put(key, ("val-" + i).getBytes(StandardCharsets.UTF_8)).toBytes());
        }

        PrepareSplitCommandProto prep = PrepareSplitCommandProto.newBuilder()
                .setSplitOperationId("split-op-3")
                .setParentShardId("shard-0")
                .setChildShardId("shard-1")
                .setSplitKey(ByteString.copyFrom("key-050".getBytes(StandardCharsets.UTF_8)))
                .build();
        stateMachine.apply(11L, SplitCommandCodec.encode(prep));

        FinalizeSplitCommandProto fin = FinalizeSplitCommandProto.newBuilder()
                .setSplitOperationId("split-op-3")
                .setParentShardId("shard-0")
                .setChildShardId("shard-1")
                .setSplitKey(ByteString.copyFrom("key-050".getBytes(StandardCharsets.UTF_8)))
                .build();
        stateMachine.apply(12L, SplitCommandCodec.encode(fin));

        assertThat(stateMachine.lifecycle()).isEqualTo(ShardLifecycle.ACTIVE);

        // Parent keys below splitKey remain client-accessible
        assertThat(stateMachine.get("key-040")).isNotNull();

        // Parent keys at or above splitKey are unroutable stale data -> rejected with STALE_EPOCH
        assertThatThrownBy(() -> stateMachine.get("key-050"))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.STALE_EPOCH);

        assertThat(stateMachine.unroutableStaleKeys()).contains("key-050", "key-100");

        // Delayed GC safely prunes the unroutable stale keys
        int pruned = stateMachine.pruneStaleKeys();
        assertThat(pruned).isEqualTo(6);
        assertThat(stateMachine.unroutableStaleKeys()).isEmpty();
        assertThat(stateMachine.size()).isEqualTo(4); // key-010, 020, 030, 040
    }

    @Test
    void testChildLifecycleReadyThenActivated() {
        KeyValueStateMachine childSm = new KeyValueStateMachine();
        childSm.setLifecycle(ShardLifecycle.READY);

        // In READY state, client operations rejected with SHARD_NOT_ACTIVATED
        KvCommand cmd = KvCommand.put("key-050", "v".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> childSm.apply(1L, cmd.toBytes()))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.SHARD_NOT_ACTIVATED);

        // Activate child
        ActivateShardCommandProto act = ActivateShardCommandProto.newBuilder()
                .setSplitOperationId("split-op-4")
                .setChildShardId("shard-1")
                .build();
        childSm.apply(2L, SplitCommandCodec.encode(act));

        assertThat(childSm.lifecycle()).isEqualTo(ShardLifecycle.ACTIVE);

        // Now writes succeed
        childSm.apply(3L, cmd.toBytes());
        assertThat(childSm.containsKey("key-050")).isTrue();
    }

    @Test
    void testAbortSplitRevertsParentToActive() {
        KvCommand putCmd = KvCommand.put("key-050", "val".getBytes(StandardCharsets.UTF_8));
        stateMachine.apply(1L, putCmd.toBytes());

        PrepareSplitCommandProto prep = PrepareSplitCommandProto.newBuilder()
                .setSplitOperationId("split-op-5")
                .setParentShardId("shard-0")
                .setSplitKey(ByteString.copyFrom("key-050".getBytes(StandardCharsets.UTF_8)))
                .build();
        stateMachine.apply(2L, SplitCommandCodec.encode(prep));
        assertThat(stateMachine.lifecycle()).isEqualTo(ShardLifecycle.SPLITTING);

        // Abort
        AbortSplitCommandProto abort = AbortSplitCommandProto.newBuilder()
                .setSplitOperationId("split-op-5")
                .setParentShardId("shard-0")
                .setReason("Test abort")
                .build();
        stateMachine.apply(3L, SplitCommandCodec.encode(abort));

        assertThat(stateMachine.lifecycle()).isEqualTo(ShardLifecycle.ACTIVE);
        assertThat(stateMachine.splitKey()).isNull();

        // Writes at key-050 now succeed again
        KvCommand putNew = KvCommand.put("key-050", "val-after-abort".getBytes(StandardCharsets.UTF_8));
        stateMachine.apply(4L, putNew.toBytes());
        assertThat(new String(stateMachine.get("key-050"), StandardCharsets.UTF_8)).isEqualTo("val-after-abort");
    }

    @Test
    void testAbortBootstrapTombstonesChild() {
        KeyValueStateMachine childSm = new KeyValueStateMachine();
        childSm.setLifecycle(ShardLifecycle.BOOTSTRAPPING);

        AbortBootstrapCommandProto abort = AbortBootstrapCommandProto.newBuilder()
                .setSplitOperationId("split-op-6")
                .setChildShardId("shard-1")
                .setReason("Bootstrap failure")
                .build();

        childSm.apply(1L, SplitCommandCodec.encode(abort));
        assertThat(childSm.lifecycle()).isEqualTo(ShardLifecycle.TOMBSTONED);

        KvCommand cmd = KvCommand.put("key-050", "val".getBytes(StandardCharsets.UTF_8));
        assertThatThrownBy(() -> childSm.apply(2L, cmd.toBytes()))
                .isInstanceOf(DatabaseException.class)
                .matches(e -> ((DatabaseException) e).getErrorCode() == ErrorCode.NODE_STOPPED);
    }
}
