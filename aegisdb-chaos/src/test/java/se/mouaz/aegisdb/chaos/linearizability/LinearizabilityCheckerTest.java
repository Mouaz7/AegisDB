package se.mouaz.aegisdb.chaos.linearizability;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("WGL Linearizability Verification Suite")
class LinearizabilityCheckerTest {

    private final LinearizabilityChecker checker = new LinearizabilityChecker();

    @Test
    @DisplayName("Empty trace is vacuously linearizable")
    void emptyTraceIsLinearizable() {
        LinearizabilityResult result = checker.verify(List.of());
        assertThat(result.isLinearizable()).isTrue();
        assertThat(result.totalOperations()).isZero();
    }

    @Test
    @DisplayName("Sequential Read-after-Write is linearizable")
    void sequentialReadAfterWriteIsLinearizable() {
        RecordedOperation w1 = RecordedOperation.write(1, "c1", "x", "v1", 100, 200, OperationStatus.OK);
        RecordedOperation r1 = RecordedOperation.read(2, "c2", "x", "v1", 250, 300);

        LinearizabilityResult result = checker.verify(List.of(w1, r1));
        assertThat(result.isLinearizable()).isTrue();
        assertThat(result.witnessExecution()).containsExactly(w1, r1);
    }

    @Test
    @DisplayName("Stale read violating real-time order fails linearizability")
    void staleReadFailsLinearizability() {
        // W1 writes x=1 from 100..200
        RecordedOperation w1 = RecordedOperation.write(1, "c1", "x", "1", 100, 200, OperationStatus.OK);
        // W2 writes x=2 from 300..400
        RecordedOperation w2 = RecordedOperation.write(2, "c2", "x", "2", 300, 400, OperationStatus.OK);
        // R1 starts at 500 (strictly after W2 returned), but observes stale value x=1
        RecordedOperation r1 = RecordedOperation.read(3, "c3", "x", "1", 500, 600);

        LinearizabilityResult result = checker.verify(List.of(w1, w2, r1));
        assertThat(result.isLinearizable()).isFalse();
        assertThat(result.violationDetails()).contains("no legal sequential execution satisfies real-time ordering");
    }

    @Test
    @DisplayName("Concurrent overlapping read and write linearizes correctly")
    void concurrentOverlappingReadAndWrite() {
        // W1 invokes at 100, returns at 500
        RecordedOperation w1 = RecordedOperation.write(1, "c1", "k", "val", 100, 500, OperationStatus.OK);
        // R1 invokes at 200, returns at 300, observes null (before W1 linearized)
        RecordedOperation r1 = RecordedOperation.read(2, "c2", "k", null, 200, 300);
        // R2 invokes at 350, returns at 450, observes "val" (after W1 linearized)
        RecordedOperation r2 = RecordedOperation.read(3, "c3", "k", "val", 350, 450);

        LinearizabilityResult result = checker.verify(List.of(w1, r1, r2));
        assertThat(result.isLinearizable()).isTrue();
        // Valid linearization order: R1 (null) -> W1 ("val") -> R2 ("val")
        assertThat(result.witnessExecution()).containsExactly(r1, w1, r2);
    }

    @Test
    @DisplayName("Timed-out indeterminate write that actually committed is linearizable")
    void indeterminateWriteThatCommitted() {
        // W1 times out at 300, but actually reached quorum
        RecordedOperation w1 = RecordedOperation.write(1, "c1", "acc", "100", 100, 300, OperationStatus.TIMEOUT_INDETERMINATE);
        // Subsequent read at 400 observes "100"
        RecordedOperation r1 = RecordedOperation.read(2, "c2", "acc", "100", 400, 500);

        LinearizabilityResult result = checker.verify(List.of(w1, r1));
        assertThat(result.isLinearizable()).isTrue();
        assertThat(result.witnessExecution()).containsExactly(w1, r1);
    }

    @Test
    @DisplayName("Timed-out indeterminate write that was dropped is linearizable")
    void indeterminateWriteThatWasDropped() {
        // W1 times out at 300 and was dropped (never committed)
        RecordedOperation w1 = RecordedOperation.write(1, "c1", "acc", "100", 100, 300, OperationStatus.TIMEOUT_INDETERMINATE);
        // Subsequent read at 400 observes null (never committed)
        RecordedOperation r1 = RecordedOperation.read(2, "c2", "acc", null, 400, 500);

        LinearizabilityResult result = checker.verify(List.of(w1, r1));
        assertThat(result.isLinearizable()).isTrue();
        // Witness only needs to execute the confirmed read against initial empty state
        assertThat(result.witnessExecution()).containsExactly(r1);
    }

    @Test
    @DisplayName("CAS operations respect atomic compare-and-swap semantics")
    void casOperationsLinearizable() {
        RecordedOperation w1 = RecordedOperation.write(1, "c1", "ctr", "0", 100, 200, OperationStatus.OK);
        RecordedOperation cas1 = RecordedOperation.cas(2, "c2", "ctr", "0", "1", 250, 350, OperationStatus.OK);
        RecordedOperation cas2 = RecordedOperation.cas(3, "c3", "ctr", "0", "2", 360, 450, OperationStatus.FAIL);
        RecordedOperation r1 = RecordedOperation.read(4, "c4", "ctr", "1", 460, 500);

        LinearizabilityResult result = checker.verify(List.of(w1, cas1, cas2, r1));
        assertThat(result.isLinearizable()).isTrue();
        assertThat(result.witnessExecution()).containsExactly(w1, cas1, cas2, r1);
    }
}
