package se.mouaz.aegisdb.chaos.linearizability;

import java.util.Collections;
import java.util.List;

/**
 * Result of a linearizability verification.
 */
public record LinearizabilityResult(
        boolean isLinearizable,
        int totalOperations,
        int linearizedOperations,
        List<RecordedOperation> witnessExecution,
        String violationDetails
) {
    public static LinearizabilityResult success(int totalOps, List<RecordedOperation> witness) {
        return new LinearizabilityResult(true, totalOps, witness.size(), Collections.unmodifiableList(witness), null);
    }

    public static LinearizabilityResult violation(int totalOps, int linearizedCount, String details) {
        return new LinearizabilityResult(false, totalOps, linearizedCount, Collections.emptyList(), details);
    }
}
