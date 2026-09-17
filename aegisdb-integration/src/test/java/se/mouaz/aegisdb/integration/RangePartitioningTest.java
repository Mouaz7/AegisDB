package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ByteArrayKey;
import se.mouaz.aegisdb.common.KeyBound;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.ShardLifecycle;
import se.mouaz.aegisdb.sharding.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit and invariant tests for Range Partitioning & Topology Management
 * (Phase 1 Multi-Raft Architecture, §28).
 */
class RangePartitioningTest {

    @Test
    @DisplayName("ByteArrayKey enforces unsigned lexicographical byte ordering and immutability")
    void testByteArrayKeyOrderingAndImmutability() {
        byte[] arr1 = new byte[]{(byte) 0x00, (byte) 0x05};
        byte[] arr2 = new byte[]{(byte) 0x7F};
        byte[] arr3 = new byte[]{(byte) 0x80}; // In signed byte, 0x80 is -128, but unsigned is 128 > 127
        byte[] arr4 = new byte[]{(byte) 0xFF};

        ByteArrayKey key1 = ByteArrayKey.of(arr1);
        ByteArrayKey key2 = ByteArrayKey.of(arr2);
        ByteArrayKey key3 = ByteArrayKey.of(arr3);
        ByteArrayKey key4 = ByteArrayKey.of(arr4);

        assertThat(key1.compareTo(key2)).isNegative();
        assertThat(key2.compareTo(key3)).isNegative(); // 0x7F < 0x80 in unsigned order
        assertThat(key3.compareTo(key4)).isNegative();

        // Immutability check: modifying original array must not mutate key
        byte[] mutable = new byte[]{1, 2, 3};
        ByteArrayKey immutableKey = ByteArrayKey.of(mutable);
        mutable[0] = 99;
        assertThat(immutableKey.getBytes()[0]).isEqualTo((byte) 1);

        // Modifying returned array must not mutate key
        immutableKey.getBytes()[0] = 77;
        assertThat(immutableKey.getBytes()[0]).isEqualTo((byte) 1);
    }

    @Test
    @DisplayName("KeyBound correctly models negative and positive infinity ordering")
    void testKeyBoundInfinityOrdering() {
        KeyBound negInf = KeyBound.negativeInfinity();
        KeyBound posInf = KeyBound.positiveInfinity();
        KeyBound keyA = KeyBound.exact("a");
        KeyBound keyZ = KeyBound.exact("z");

        assertThat(negInf.compareTo(keyA)).isNegative();
        assertThat(negInf.compareTo(keyZ)).isNegative();
        assertThat(negInf.compareTo(posInf)).isNegative();
        assertThat(negInf.compareTo(KeyBound.negativeInfinity())).isZero();

        assertThat(posInf.compareTo(keyA)).isPositive();
        assertThat(posInf.compareTo(keyZ)).isPositive();
        assertThat(posInf.compareTo(negInf)).isPositive();
        assertThat(posInf.compareTo(KeyBound.positiveInfinity())).isZero();

        assertThat(keyA.compareTo(keyZ)).isNegative();
        assertThat(keyZ.compareTo(keyA)).isPositive();
    }

    @Test
    @DisplayName("KeyRange validates valid interval [start, end) and containment")
    void testKeyRangeContainmentAndSplits() {
        KeyRange fullRange = KeyRange.fullKeyspace();
        assertThat(fullRange.startBound().isNegativeInfinity() && fullRange.endBound().isPositiveInfinity()).isTrue();
        assertThat(fullRange.contains(ByteArrayKey.of("apple"))).isTrue();
        assertThat(fullRange.contains(ByteArrayKey.of(new byte[]{(byte) 0xFF}))).isTrue();

        // Split [negativeInfinity, positiveInfinity) at "m"
        ByteArrayKey splitKey = ByteArrayKey.of("m");
        KeyRange left = KeyRange.of(KeyBound.negativeInfinity(), KeyBound.exact(splitKey));
        KeyRange right = KeyRange.of(KeyBound.exact(splitKey), KeyBound.positiveInfinity());

        assertThat(left.contains(ByteArrayKey.of("apple"))).isTrue();
        assertThat(left.contains(ByteArrayKey.of("m"))).isFalse(); // half-open [start, end)
        assertThat(right.contains(ByteArrayKey.of("m"))).isTrue();
        assertThat(right.contains(ByteArrayKey.of("zebra"))).isTrue();

        assertThat(left.overlaps(right)).isFalse();

        // Invalid range: startBound >= endBound
        assertThatThrownBy(() -> KeyRange.of(KeyBound.exact("z"), KeyBound.exact("a")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("TopologySnapshot enforces SingleAuthoritativeOwner, NoOverlap, and NoGaps")
    void testTopologySnapshotAuthoritativeInvariants() {
        ShardId s1 = ShardId.of("shard-1");
        ShardId s2 = ShardId.of("shard-2");

        ByteArrayKey splitKey = ByteArrayKey.of("m");
        KeyRange r1 = KeyRange.of(KeyBound.negativeInfinity(), KeyBound.exact(splitKey));
        KeyRange r2 = KeyRange.of(KeyBound.exact(splitKey), KeyBound.positiveInfinity());

        List<TopologySnapshot.ShardRangeAssignment> assignments = List.of(
                new TopologySnapshot.ShardRangeAssignment(s1, r1, ShardEpoch.initial(), ShardLifecycle.ACTIVE),
                new TopologySnapshot.ShardRangeAssignment(s2, r2, ShardEpoch.initial(), ShardLifecycle.ACTIVE)
        );

        List<ShardMetadata> metadata = List.of(
                new ShardMetadata(s1, r1, ShardEpoch.initial(), ShardLifecycle.ACTIVE, java.time.Instant.now(), 3, Collections.emptyMap()),
                new ShardMetadata(s2, r2, ShardEpoch.initial(), ShardLifecycle.ACTIVE, java.time.Instant.now(), 3, Collections.emptyMap())
        );

        TopologySnapshot snapshot = new TopologySnapshot(TopologyVersion.of(2), assignments, metadata);
        assertThat(snapshot.version().version()).isEqualTo(2);
        assertThat(snapshot.findAuthoritativeShard(ByteArrayKey.of("cat")).map(TopologySnapshot.ShardRangeAssignment::shardId)).contains(s1);
        assertThat(snapshot.findAuthoritativeShard(ByteArrayKey.of("noon")).map(TopologySnapshot.ShardRangeAssignment::shardId)).contains(s2);

        // Gap detection: missing [m, p)
        KeyRange r2Gap = KeyRange.of(KeyBound.exact("p"), KeyBound.positiveInfinity());
        List<TopologySnapshot.ShardRangeAssignment> gapAssignments = List.of(
                new TopologySnapshot.ShardRangeAssignment(s1, r1, ShardEpoch.initial(), ShardLifecycle.ACTIVE),
                new TopologySnapshot.ShardRangeAssignment(s2, r2Gap, ShardEpoch.initial(), ShardLifecycle.ACTIVE)
        );
        assertThatThrownBy(() -> new TopologySnapshot(TopologyVersion.of(3), gapAssignments, metadata))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("NoAuthoritativeGaps");

        // Overlap detection
        KeyRange r2Overlap = KeyRange.of(KeyBound.exact("k"), KeyBound.positiveInfinity());
        List<TopologySnapshot.ShardRangeAssignment> overlapAssignments = List.of(
                new TopologySnapshot.ShardRangeAssignment(s1, r1, ShardEpoch.initial(), ShardLifecycle.ACTIVE),
                new TopologySnapshot.ShardRangeAssignment(s2, r2Overlap, ShardEpoch.initial(), ShardLifecycle.ACTIVE)
        );
        assertThatThrownBy(() -> new TopologySnapshot(TopologyVersion.of(3), overlapAssignments, metadata))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("NoAuthoritativeOverlap");
    }

    @Test
    @DisplayName("RangePartitioner routes keys dynamically as topology snapshot updates")
    void testRangePartitionerDynamicUpdates() {
        ShardId s1 = ShardId.of("shard-1");
        TopologySnapshot initial = TopologySnapshot.initialSingleShard(s1, 3);
        RangePartitioner partitioner = new RangePartitioner(initial);

        ShardMap shardMap = new ShardMap();
        shardMap.registerShard(new Shard(s1, initial.metadataMap().get(s1),
                ReplicationGroup.of(s1, se.mouaz.aegisdb.common.NodeId.of("node-1"))));

        // All keys route to s1 initially
        assertThat(partitioner.selectShard("apple", shardMap)).isEqualTo(s1);
        assertThat(partitioner.selectShard("zebra", shardMap)).isEqualTo(s1);

        // Perform split: s1 -> s1 (left) and s2 (right)
        ShardId s2 = ShardId.of("shard-2");
        ByteArrayKey splitKey = ByteArrayKey.of("m");
        KeyRange left = KeyRange.of(KeyBound.negativeInfinity(), KeyBound.exact(splitKey));
        KeyRange right = KeyRange.of(KeyBound.exact(splitKey), KeyBound.positiveInfinity());

        List<TopologySnapshot.ShardRangeAssignment> newAssignments = List.of(
                new TopologySnapshot.ShardRangeAssignment(s1, left, ShardEpoch.of(1, 1), ShardLifecycle.ACTIVE),
                new TopologySnapshot.ShardRangeAssignment(s2, right, ShardEpoch.initial(), ShardLifecycle.ACTIVE)
        );

        List<ShardMetadata> newMetadata = List.of(
                new ShardMetadata(s1, left, ShardEpoch.of(1, 1), ShardLifecycle.ACTIVE, java.time.Instant.now(), 3, Collections.emptyMap()),
                new ShardMetadata(s2, right, ShardEpoch.initial(), ShardLifecycle.ACTIVE, java.time.Instant.now(), 3, Collections.emptyMap())
        );

        shardMap.registerShard(new Shard(s2, newMetadata.get(1),
                ReplicationGroup.of(s2, se.mouaz.aegisdb.common.NodeId.of("node-2"))));

        partitioner.updateSnapshot(new TopologySnapshot(TopologyVersion.of(2), newAssignments, newMetadata));

        assertThat(partitioner.selectShard("apple", shardMap)).isEqualTo(s1);
        assertThat(partitioner.selectShard("zebra", shardMap)).isEqualTo(s2);
    }

    @Test
    @DisplayName("MetadataRaftGroup commitCutover advances version, updates snapshot and notifies listeners")
    void testMetadataRaftGroupCommitCutover() {
        ShardId parentId = ShardId.of("shard-parent");
        MetadataRaftGroup metaRaft = new MetadataRaftGroup(parentId, 3);

        AtomicInteger notificationCount = new AtomicInteger(0);
        metaRaft.addListener(snapshot -> notificationCount.incrementAndGet());

        ShardId childId = ShardId.of("shard-child");
        ByteArrayKey splitKey = ByteArrayKey.of("m");
        KeyRange parentNewRange = KeyRange.of(KeyBound.negativeInfinity(), KeyBound.exact(splitKey));
        KeyRange childNewRange = KeyRange.of(KeyBound.exact(splitKey), KeyBound.positiveInfinity());

        MetadataRaftGroup.TopologyCutover cutover = new MetadataRaftGroup.TopologyCutover(
                "split-op-99",
                1L, // expected topology version
                parentId,
                "rg-parent",
                parentNewRange,
                ShardEpoch.of(1, 1),
                childId,
                "rg-child",
                childNewRange,
                ShardEpoch.initial(),
                150L,
                new byte[]{1, 2, 3, 4}
        );

        TopologySnapshot updatedSnapshot = metaRaft.commitCutover(cutover);

        assertThat(updatedSnapshot.version().version()).isEqualTo(2L);
        assertThat(notificationCount.get()).isEqualTo(1);
        assertThat(updatedSnapshot.findAuthoritativeShard(ByteArrayKey.of("apple")).map(TopologySnapshot.ShardRangeAssignment::shardId)).contains(parentId);
        assertThat(updatedSnapshot.findAuthoritativeShard(ByteArrayKey.of("zebra")).map(TopologySnapshot.ShardRangeAssignment::shardId)).contains(childId);

        // Reject stale proposals with mismatched expected version
        assertThatThrownBy(() -> metaRaft.commitCutover(cutover))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Stale topology proposal");
    }
}
