package se.mouaz.aegisdb.mvcc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MVCC Visibility Rule Tests")
class VisibilityRuleTest {

    private static final byte[] VAL1 = "val1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VAL2 = "val2".getBytes(StandardCharsets.UTF_8);

    @Test
    @DisplayName("Committed version at or before snapshot timestamp is visible")
    void committedVersionBeforeSnapshotIsVisible() {
        Snapshot snapshot = Snapshot.of(10L);
        VersionedValue v = new VersionedValue(1L, 5L, VAL1, false);

        assertThat(VisibilityRule.isVisible(v, snapshot)).isTrue();
    }

    @Test
    @DisplayName("Committed version at exact snapshot timestamp is visible")
    void committedVersionAtSnapshotIsVisible() {
        Snapshot snapshot = Snapshot.of(10L);
        VersionedValue v = new VersionedValue(1L, 10L, VAL1, false);

        assertThat(VisibilityRule.isVisible(v, snapshot)).isTrue();
    }

    @Test
    @DisplayName("Future committed version is invisible (commitTimestamp > snapshot.readTimestamp)")
    void futureVersionIsInvisible() {
        Snapshot snapshot = Snapshot.of(10L);
        VersionedValue future = new VersionedValue(1L, 11L, VAL1, false);

        assertThat(VisibilityRule.isVisible(future, snapshot)).isFalse();
    }

    @Test
    @DisplayName("Uncommitted version from another transaction is invisible (dirty read prevention)")
    void uncommittedOtherTxIsInvisible() {
        Snapshot snapshot = Snapshot.of(10L);
        VersionedValue uncommitted = new VersionedValue(99L, VersionedValue.UNCOMMITTED, VAL1, false);

        assertThat(VisibilityRule.isVisible(uncommitted, snapshot)).isFalse();
    }

    @Test
    @DisplayName("Own uncommitted version is visible to writing transaction (read-your-own-writes)")
    void ownUncommittedIsVisible() {
        Snapshot txSnapshot = Snapshot.forTransaction(10L, 42L, Set.of());
        VersionedValue ownUncommitted = new VersionedValue(42L, VersionedValue.UNCOMMITTED, VAL1, false);

        assertThat(VisibilityRule.isVisible(ownUncommitted, txSnapshot)).isTrue();
    }

    @Test
    @DisplayName("Aborted version is never visible under any circumstances")
    void abortedVersionIsNeverVisible() {
        Snapshot snapshot = Snapshot.of(10L);
        Snapshot ownTxSnapshot = Snapshot.forTransaction(10L, 42L, Set.of());

        VersionedValue aborted = new VersionedValue(42L, VersionedValue.ABORTED, VAL1, false);

        assertThat(VisibilityRule.isVisible(aborted, snapshot)).isFalse();
        assertThat(VisibilityRule.isVisible(aborted, ownTxSnapshot)).isFalse();
    }

    @Test
    @DisplayName("Version from transaction active at snapshot establishment is invisible")
    void txActiveAtSnapshotStartIsInvisible() {
        // Transaction 77 was active when snapshot 10 was taken
        Snapshot snapshot = Snapshot.of(10L, Set.of(77L));
        // Even if tx 77 subsequently committed at timestamp 8 (before snapshot timestamp 10)
        VersionedValue v = new VersionedValue(77L, 8L, VAL1, false);

        assertThat(VisibilityRule.isVisible(v, snapshot)).isFalse();
    }

    @Test
    @DisplayName("Null version or null snapshot safely returns false")
    void nullHandling() {
        Snapshot snapshot = Snapshot.of(10L);
        VersionedValue v = new VersionedValue(1L, 5L, VAL1, false);

        assertThat(VisibilityRule.isVisible(null, snapshot)).isFalse();
        assertThat(VisibilityRule.isVisible(v, null)).isFalse();
    }
}
