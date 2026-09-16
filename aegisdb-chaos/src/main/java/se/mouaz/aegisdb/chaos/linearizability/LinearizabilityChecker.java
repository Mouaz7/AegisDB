package se.mouaz.aegisdb.chaos.linearizability;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * In-process Wing & Lowe (WGL) linearizability verifier.
 * Evaluates whether concurrent execution histories of Key-Value operations
 * are linearizable with respect to real-time precedence and sequential semantics.
 *
 * <p>Crucially handles indeterminate timeouts: operations whose outcomes are unknown
 * (e.g. timeout during leader failover) are branched such that they either linearized
 * or were dropped before commit.
 */
public final class LinearizabilityChecker {

    private static final Logger log = LoggerFactory.getLogger(LinearizabilityChecker.class);

    private final long searchTimeoutMs;

    public LinearizabilityChecker() {
        this(15_000L);
    }

    public LinearizabilityChecker(long searchTimeoutMs) {
        this.searchTimeoutMs = searchTimeoutMs;
    }

    /**
     * Verifies linearizability of the provided operations.
     * Operations on disjoint keys commute and are verified independently.
     */
    public LinearizabilityResult verify(List<RecordedOperation> operations) {
        if (operations == null || operations.isEmpty()) {
            return LinearizabilityResult.success(0, Collections.emptyList());
        }

        // 1. Partition operations by key
        Map<String, List<RecordedOperation>> byKey = operations.stream()
                .collect(Collectors.groupingBy(RecordedOperation::key));

        List<RecordedOperation> globalWitness = new ArrayList<>();
        int totalLinearized = 0;

        for (Map.Entry<String, List<RecordedOperation>> entry : byKey.entrySet()) {
            String key = entry.getKey();
            List<RecordedOperation> keyOps = entry.getValue();

            LinearizabilityResult keyResult = verifyKeyHistory(key, keyOps);
            if (!keyResult.isLinearizable()) {
                log.warn("Linearizability violation on key '{}': {}", key, keyResult.violationDetails());
                return keyResult;
            }
            globalWitness.addAll(keyResult.witnessExecution());
            totalLinearized += keyResult.linearizedOperations();
        }

        return LinearizabilityResult.success(operations.size(), globalWitness);
    }

    private LinearizabilityResult verifyKeyHistory(String key, List<RecordedOperation> ops) {
        // Sort operations topologically or by invocation time as index reference
        List<RecordedOperation> indexedOps = new ArrayList<>(ops);
        indexedOps.sort(Comparator.comparingLong(RecordedOperation::invokeTimeNs));

        int n = indexedOps.size();
        if (n == 0) {
            return LinearizabilityResult.success(0, Collections.emptyList());
        }

        // Precompute precedence matrix:
        // prec[i][j] is true iff indexedOps[i] must linearize before indexedOps[j] (returnTime[i] < invokeTime[j])
        boolean[][] prec = new boolean[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (i != j && indexedOps.get(i).returnTimeNs() < indexedOps.get(j).invokeTimeNs()) {
                    prec[i][j] = true;
                }
            }
        }

        // Memoization set: tracks (remainingOperationsBitSet, modelStateHash)
        Set<SearchState> visited = new HashSet<>();
        List<RecordedOperation> witness = new ArrayList<>();
        BitSet remaining = new BitSet(n);
        remaining.set(0, n);

        long deadline = System.currentTimeMillis() + searchTimeoutMs;

        boolean success = search(indexedOps, prec, remaining, new KvModel(), witness, visited, deadline);

        if (success) {
            return LinearizabilityResult.success(n, witness);
        } else {
            String details = String.format(
                    "Key '%s': no legal sequential execution satisfies real-time ordering across %d operations.",
                    key, n);
            return LinearizabilityResult.violation(n, witness.size(), details);
        }
    }

    private boolean search(List<RecordedOperation> ops,
                           boolean[][] prec,
                           BitSet remaining,
                           KvModel currentModel,
                           List<RecordedOperation> witness,
                           Set<SearchState> visited,
                           long deadline) {

        if (remaining.isEmpty()) {
            return true;
        }

        if (System.currentTimeMillis() > deadline) {
            log.warn("Linearizability verification search budget exceeded (timeout: {}ms)", searchTimeoutMs);
            return false;
        }

        SearchState state = new SearchState(remaining, currentModel.state());
        if (!visited.add(state)) {
            return false; // Already explored this sub-configuration
        }

        // Find candidate operations: an op in 'remaining' is a candidate if no other op in 'remaining' precedes it
        List<Integer> candidates = new ArrayList<>();
        for (int i = remaining.nextSetBit(0); i >= 0; i = remaining.nextSetBit(i + 1)) {
            boolean hasPredecessor = false;
            for (int j = remaining.nextSetBit(0); j >= 0; j = remaining.nextSetBit(j + 1)) {
                if (i != j && prec[j][i]) {
                    hasPredecessor = true;
                    break;
                }
            }
            if (!hasPredecessor) {
                candidates.add(i);
            }
        }

        for (int candidateIdx : candidates) {
            RecordedOperation op = ops.get(candidateIdx);

            if (op.status() == OperationStatus.TIMEOUT_INDETERMINATE) {
                // Branch 1: The indeterminate write/CAS took effect at this linearization point
                Optional<KvModel> nextModelOpt = currentModel.apply(op);
                if (nextModelOpt.isPresent()) {
                    remaining.clear(candidateIdx);
                    witness.add(op);

                    if (search(ops, prec, remaining, nextModelOpt.get(), witness, visited, deadline)) {
                        return true;
                    }

                    witness.remove(witness.size() - 1);
                    remaining.set(candidateIdx);
                }

                // Branch 2: The indeterminate write/CAS was dropped before commit (never took effect)
                remaining.clear(candidateIdx);
                if (search(ops, prec, remaining, currentModel, witness, visited, deadline)) {
                    return true;
                }
                remaining.set(candidateIdx);

            } else {
                // Operation completed definitively (OK or explicit FAIL)
                Optional<KvModel> nextModelOpt = currentModel.apply(op);
                if (nextModelOpt.isPresent()) {
                    remaining.clear(candidateIdx);
                    witness.add(op);

                    if (search(ops, prec, remaining, nextModelOpt.get(), witness, visited, deadline)) {
                        return true;
                    }

                    witness.remove(witness.size() - 1);
                    remaining.set(candidateIdx);
                }
            }
        }

        return false;
    }

    private record SearchState(BitSet remainingOps, Map<String, String> modelState) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SearchState that)) return false;
            return Objects.equals(remainingOps, that.remainingOps) &&
                    Objects.equals(modelState, that.modelState);
        }

        @Override
        public int hashCode() {
            return 31 * remainingOps.hashCode() + (modelState != null ? modelState.hashCode() : 0);
        }
    }
}
