package se.mouaz.aegisdb.chaos.linearizability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe operation trace collector that records concurrent client executions
 * for subsequent linearizability verification.
 */
public final class OperationTrace {

    private final AtomicInteger idSequence = new AtomicInteger(1);
    private final ConcurrentLinkedQueue<RecordedOperation> trace = new ConcurrentLinkedQueue<>();
    private final LinearizabilityChecker checker;

    public OperationTrace() {
        this(new LinearizabilityChecker());
    }

    public OperationTrace(LinearizabilityChecker checker) {
        this.checker = checker;
    }

    public int nextId() {
        return idSequence.getAndIncrement();
    }

    public void record(RecordedOperation op) {
        trace.add(op);
    }

    public void recordRead(String clientId, String key, String observedValue, long invokeTimeNs, long returnTimeNs) {
        trace.add(RecordedOperation.read(nextId(), clientId, key, observedValue, invokeTimeNs, returnTimeNs));
    }

    public void recordWrite(String clientId, String key, String value, long invokeTimeNs, long returnTimeNs, OperationStatus status) {
        trace.add(RecordedOperation.write(nextId(), clientId, key, value, invokeTimeNs, returnTimeNs, status));
    }

    public void recordCas(String clientId, String key, String expectedVal, String newVal,
                          long invokeTimeNs, long returnTimeNs, OperationStatus status) {
        trace.add(RecordedOperation.cas(nextId(), clientId, key, expectedVal, newVal, invokeTimeNs, returnTimeNs, status));
    }

    public List<RecordedOperation> operations() {
        return Collections.unmodifiableList(new ArrayList<>(trace));
    }

    public int size() {
        return trace.size();
    }

    public void clear() {
        trace.clear();
        idSequence.set(1);
    }

    public LinearizabilityResult verifyLinearizability() {
        return checker.verify(new ArrayList<>(trace));
    }
}
