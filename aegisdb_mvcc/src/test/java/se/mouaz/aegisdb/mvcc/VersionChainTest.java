package se.mouaz.aegisdb.mvcc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MVCC Version Chain Tests")
class VersionChainTest {

    private static final byte[] VAL_V1 = "v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VAL_V2 = "v2".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VAL_V3 = "v3".getBytes(StandardCharsets.UTF_8);

    @Test
    @DisplayName("Append orders versions newest to oldest")
    void appendOrdersNewestFirst() {
        VersionChain chain = new VersionChain("user:1");
        chain.append(new VersionedValue(1L, 10L, VAL_V1, false));
        chain.append(new VersionedValue(2L, 20L, VAL_V2, false));

        assertThat(chain.versionCount()).isEqualTo(2);
        assertThat(chain.head().commitTimestamp()).isEqualTo(20L);
        assertThat(chain.head().next().commitTimestamp()).isEqualTo(10L);
    }

    @Test
    @DisplayName("Snapshot sees historically correct version")
    void snapshotTraversesChainToCorrectVersion() {
        VersionChain chain = new VersionChain("balance");
        chain.append(new VersionedValue(1L, 10L, VAL_V1, false));
        chain.append(new VersionedValue(2L, 20L, VAL_V2, false));
        chain.append(new VersionedValue(3L, 30L, VAL_V3, false));

        // Snapshot at T=15 sees v1
        Optional<byte[]> atT15 = chain.findVisible(Snapshot.of(15L));
        assertThat(atT15).isPresent().contains(VAL_V1);

        // Snapshot at T=25 sees v2
        Optional<byte[]> atT25 = chain.findVisible(Snapshot.of(25L));
        assertThat(atT25).isPresent().contains(VAL_V2);

        // Snapshot at T=35 sees v3
        Optional<byte[]> atT35 = chain.findVisible(Snapshot.of(35L));
        assertThat(atT35).isPresent().contains(VAL_V3);

        // Snapshot at T=5 sees nothing (before v1)
        Optional<byte[]> atT5 = chain.findVisible(Snapshot.of(5L));
        assertThat(atT5).isEmpty();
    }

    @Test
    @DisplayName("Tombstone returns empty for snapshots after deletion")
    void tombstoneDeletesKeyAtTimestamp() {
        VersionChain chain = new VersionChain("temp");
        chain.append(new VersionedValue(1L, 10L, VAL_V1, false));
        chain.append(new VersionedValue(2L, 20L, new byte[0], true)); // deleted at T=20

        assertThat(chain.findVisible(Snapshot.of(15L))).isPresent().contains(VAL_V1);
        assertThat(chain.findVisible(Snapshot.of(25L))).isEmpty();
    }

    @Test
    @DisplayName("Commit transitions uncommitted version to committed")
    void commitVersionTransitionsCorrectly() {
        VersionChain chain = new VersionChain("key1");
        chain.append(new VersionedValue(1L, 10L, VAL_V1, false));
        chain.append(new VersionedValue(2L, VersionedValue.UNCOMMITTED, VAL_V2, false));

        assertThat(chain.findVisible(Snapshot.of(15L))).isPresent().contains(VAL_V1);
        // External reader cannot see uncommitted v2
        assertThat(chain.findVisible(Snapshot.of(100L))).isPresent().contains(VAL_V1);

        // Commit transaction 2 at T=50
        boolean committed = chain.commitVersion(2L, 50L);
        assertThat(committed).isTrue();

        // Now snapshot at 60 sees v2
        assertThat(chain.findVisible(Snapshot.of(60L))).isPresent().contains(VAL_V2);
        // But snapshot at 40 still sees v1
        assertThat(chain.findVisible(Snapshot.of(40L))).isPresent().contains(VAL_V1);
    }

    @Test
    @DisplayName("Abort removes uncommitted version from chain")
    void abortVersionRemovesUncommitted() {
        VersionChain chain = new VersionChain("key1");
        chain.append(new VersionedValue(1L, 10L, VAL_V1, false));
        chain.append(new VersionedValue(2L, VersionedValue.UNCOMMITTED, VAL_V2, false));

        assertThat(chain.versionCount()).isEqualTo(2);

        boolean aborted = chain.abortVersion(2L);
        assertThat(aborted).isTrue();

        assertThat(chain.versionCount()).isEqualTo(1);
        assertThat(chain.head().commitTimestamp()).isEqualTo(10L);
    }

    @Test
    @DisplayName("Watermark pruning retains versions >= watermark plus one baseline")
    void watermarkPruningRetainsBaseline() {
        VersionChain chain = new VersionChain("counter");
        // Created versions at 10, 20, 30, 40
        chain.append(new VersionedValue(1L, 10L, VAL_V1, false));
        chain.append(new VersionedValue(2L, 20L, VAL_V2, false));
        chain.append(new VersionedValue(3L, 30L, VAL_V3, false));
        chain.append(new VersionedValue(4L, 40L, "v4".getBytes(StandardCharsets.UTF_8), false));

        // Watermark is 25.
        // Versions >= 25: T=40, T=30.
        // Baseline version < 25: T=20 (single newest committed before watermark).
        // Pruned: T=10.
        int pruned = chain.pruneOlderThan(25L);
        assertThat(pruned).isEqualTo(1);
        assertThat(chain.versionCount()).isEqualTo(3);

        // Reader at watermark 25 can still read T=20
        assertThat(chain.findVisible(Snapshot.of(25L))).isPresent().contains(VAL_V2);
        // Reader at 35 reads T=30
        assertThat(chain.findVisible(Snapshot.of(35L))).isPresent().contains(VAL_V3);
    }
}
