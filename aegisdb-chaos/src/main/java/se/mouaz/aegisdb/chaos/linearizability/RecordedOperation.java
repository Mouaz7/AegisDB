package se.mouaz.aegisdb.chaos.linearizability;

import java.util.Objects;

/**
 * Immutable recorded operation representing an invocation-return interval in real time.
 */
public record RecordedOperation(
        int id,
        String clientId,
        OperationType type,
        String key,
        String value,
        String expectedValue,
        long invokeTimeNs,
        long returnTimeNs,
        OperationStatus status
) implements Comparable<RecordedOperation> {

    public RecordedOperation {
        Objects.requireNonNull(clientId, "clientId must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(status, "status must not be null");
    }

    public static RecordedOperation read(int id, String clientId, String key, String observedValue,
                                         long invokeTimeNs, long returnTimeNs) {
        return new RecordedOperation(id, clientId, OperationType.READ, key, observedValue, null,
                invokeTimeNs, returnTimeNs, OperationStatus.OK);
    }

    public static RecordedOperation write(int id, String clientId, String key, String value,
                                          long invokeTimeNs, long returnTimeNs, OperationStatus status) {
        return new RecordedOperation(id, clientId, OperationType.WRITE, key, value, null,
                invokeTimeNs, returnTimeNs, status);
    }

    public static RecordedOperation cas(int id, String clientId, String key, String expectedValue,
                                        String newValue, long invokeTimeNs, long returnTimeNs, OperationStatus status) {
        return new RecordedOperation(id, clientId, OperationType.CAS, key, newValue, expectedValue,
                invokeTimeNs, returnTimeNs, status);
    }

    @Override
    public int compareTo(RecordedOperation o) {
        return Long.compare(this.invokeTimeNs, o.invokeTimeNs);
    }

    @Override
    public String toString() {
        return "Op#" + id + "[" + type + " k=" + key +
                (expectedValue != null ? " exp=" + expectedValue : "") +
                (value != null ? " v=" + value : "") +
                " " + status + " (" + invokeTimeNs + ".." + returnTimeNs + "ns)]";
    }
}
