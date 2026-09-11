package se.mouaz.aegisdb.mvcc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("MVCC Garbage Collector Safety Tests")
class MvccGarbageCollectorTest {

    private MvccStore store;
    private MvccGarbageCollector gc;

    @BeforeEach
    void setUp() {
        store = new MvccStore();
        gc = new MvccGarbageCollector(store);
    }

    @Test
    @DisplayName("GC reclaims obsolete versions when no active snapshots exist")
    void gcReclaimsObsoleteVersionsWithoutSnapshots() {
        // Write 4 consecutive versions
        store.put("counter", "1".getBytes(StandardCharsets.UTF_8));
        store.put("counter", "2".getBytes(StandardCharsets.UTF_8));
        store.put("counter", "3".getBytes(StandardCharsets.UTF_8));
        store.put("counter", "4".getBytes(StandardCharsets.UTF_8));

        VersionChain chain = store.chains().get("counter");
        assertThat(chain.versionCount()).isEqualTo(4);

        // Run GC sweep
        MvccGarbageCollector.GcStats stats = gc.collectGarbage();

        // 3 obsolete versions should be pruned, 1 latest baseline kept
        assertThat(stats.versionsReclaimed()).isEqualTo(3);
        assertThat(chain.versionCount()).isEqualTo(1);
        assertThat(new String(store.get("counter").get(), StandardCharsets.UTF_8)).isEqualTo("4");
    }

    @Test
    @DisplayName("CRITICAL INVARIANT: Active snapshot protects its visible version from GC pruning")
    void activeSnapshotPreventsPruningOfVisibleVersions() {
        // Step 1: Initial write
        store.put("document", "version-1".getBytes(StandardCharsets.UTF_8));

        // Step 2: Long-running reader establishes a snapshot
        Snapshot readerSnapshot = store.createSnapshot();

        // Step 3: Writer produces multiple subsequent updates
        store.put("document", "version-2".getBytes(StandardCharsets.UTF_8));
        store.put("document", "version-3".getBytes(StandardCharsets.UTF_8));
        store.put("document", "version-4".getBytes(StandardCharsets.UTF_8));

        VersionChain chain = store.chains().get("document");
        assertThat(chain.versionCount()).isEqualTo(4);

        // Step 4: Run GC while snapshot is still open
        MvccGarbageCollector.GcStats statsDuringSnapshot = gc.collectGarbage();

        // Watermark was protected by readerSnapshot.readTimestamp
        // version-1 must NOT be pruned because readerSnapshot still needs it
        Optional<byte[]> readerVal = store.get("document", readerSnapshot);
        assertThat(readerVal).isPresent();
        assertThat(new String(readerVal.get(), StandardCharsets.UTF_8)).isEqualTo("version-1");

        // Step 5: Reader finishes and closes snapshot
        readerSnapshot.close();

        // Step 6: Subsequent GC sweep now safely reclaims obsolete historical versions
        MvccGarbageCollector.GcStats statsAfterClose = gc.collectGarbage();
        assertThat(statsAfterClose.versionsReclaimed()).isGreaterThan(0);

        // Store still reads latest version cleanly
        assertThat(new String(store.get("document").get(), StandardCharsets.UTF_8)).isEqualTo("version-4");
    }

    @Test
    @DisplayName("Tombstone key completely removed after all historical snapshots finish")
    void tombstoneKeyReclaimedWhenObsolete() {
        store.put("temp", "value".getBytes(StandardCharsets.UTF_8));
        store.delete("temp");

        assertThat(store.chains()).containsKey("temp");

        // Run GC
        MvccGarbageCollector.GcStats stats = gc.collectGarbage();
        assertThat(stats.keysReclaimed()).isEqualTo(1);
        assertThat(store.chains()).doesNotContainKey("temp");
    }
}
