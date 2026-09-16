package se.mouaz.aegisdb.chaos.linearizability;

import java.util.*;

/**
 * Sequential specification of an atomic key-value store used for linearizability verification.
 */
public final class KvModel {

    private final Map<String, String> state;

    public KvModel() {
        this(Collections.emptyMap());
    }

    public KvModel(Map<String, String> state) {
        this.state = Map.copyOf(state);
    }

    public Map<String, String> state() {
        return state;
    }

    public String get(String key) {
        return state.get(key);
    }

    /**
     * Evaluates whether the operation can be legally executed in the current sequential state,
     * returning the resulting state if legal.
     */
    public Optional<KvModel> apply(RecordedOperation op) {
        String currentVal = state.get(op.key());

        switch (op.type()) {
            case READ -> {
                // For a successful read, the observed value must equal the current sequential state
                if (Objects.equals(currentVal, op.value())) {
                    return Optional.of(this);
                }
                return Optional.empty();
            }
            case WRITE -> {
                Map<String, String> nextState = new HashMap<>(state);
                if (op.value() != null) {
                    nextState.put(op.key(), op.value());
                } else {
                    nextState.remove(op.key());
                }
                return Optional.of(new KvModel(nextState));
            }
            case CAS -> {
                boolean matchesExpected = Objects.equals(currentVal, op.expectedValue());
                if (op.status() == OperationStatus.OK) {
                    if (matchesExpected) {
                        Map<String, String> nextState = new HashMap<>(state);
                        if (op.value() != null) {
                            nextState.put(op.key(), op.value());
                        } else {
                            nextState.remove(op.key());
                        }
                        return Optional.of(new KvModel(nextState));
                    }
                    return Optional.empty();
                } else if (op.status() == OperationStatus.FAIL) {
                    // Explicit CAS failure requires that current state did NOT match expected value
                    if (!matchesExpected) {
                        return Optional.of(this);
                    }
                    return Optional.empty();
                } else {
                    // Indeterminate CAS: either succeeded if matched, or failed
                    if (matchesExpected) {
                        Map<String, String> nextState = new HashMap<>(state);
                        nextState.put(op.key(), op.value());
                        return Optional.of(new KvModel(nextState));
                    }
                    return Optional.of(this);
                }
            }
            default -> {
                return Optional.empty();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KvModel kvModel)) return false;
        return Objects.equals(state, kvModel.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state);
    }

    @Override
    public String toString() {
        return "KvModel" + state;
    }
}
