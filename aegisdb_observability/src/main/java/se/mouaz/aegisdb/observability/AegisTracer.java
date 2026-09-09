package se.mouaz.aegisdb.observability;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AegisTracer implements distributed tracing abstractions for AegisDB.
 *
 * Models the canonical trace path defined in Master Project Plan §15:
 * Client request -> routing -> leader handling -> Raft append ->
 * follower replication -> majority commit -> state machine apply -> persistence -> response
 */
public final class AegisTracer {

    private static final AegisTracer GLOBAL_INSTANCE = new AegisTracer();
    private static final AtomicLong TRACE_COUNTER = new AtomicLong(1);

    private final ConcurrentLinkedDeque<TraceSpan> completedSpans = new ConcurrentLinkedDeque<>();
    private static final int MAX_RECORDED_SPANS = 1000;

    public AegisTracer() {}

    public static AegisTracer global() {
        return GLOBAL_INSTANCE;
    }

    public static String generateTraceId() {
        return "trace-" + Long.toHexString(System.currentTimeMillis()) + "-" + TRACE_COUNTER.getAndIncrement();
    }

    public TraceSpan startSpan(String name) {
        return startSpan(generateTraceId(), null, name);
    }

    public TraceSpan startSpan(String traceId, String parentSpanId, String name) {
        return new TraceSpan(this, traceId, parentSpanId, name);
    }

    void recordSpan(TraceSpan span) {
        completedSpans.addLast(span);
        if (completedSpans.size() > MAX_RECORDED_SPANS) {
            completedSpans.pollFirst();
        }
    }

    public List<TraceSpan> getRecentSpans() {
        return new ArrayList<>(completedSpans);
    }

    public void clear() {
        completedSpans.clear();
    }

    /**
     * Immutable snapshot of an executed trace span.
     */
    public static final class TraceSpan {
        private final AegisTracer tracer;
        private final String traceId;
        private final String parentSpanId;
        private final String spanId;
        private final String name;
        private final long startTimeNanos;
        private final Map<String, String> attributes = new ConcurrentHashMap<>();
        private final List<SpanEvent> events = new ArrayList<>();
        private long endTimeNanos;
        private boolean finished = false;

        TraceSpan(AegisTracer tracer, String traceId, String parentSpanId, String name) {
            this.tracer = tracer;
            this.traceId = traceId;
            this.parentSpanId = parentSpanId;
            this.spanId = "span-" + UUID.randomUUID().toString().substring(0, 8);
            this.name = Objects.requireNonNull(name, "name must not be null");
            this.startTimeNanos = System.nanoTime();
        }

        public TraceSpan setAttribute(String key, String value) {
            attributes.put(key, value);
            return this;
        }

        public TraceSpan addEvent(String eventName) {
            events.add(new SpanEvent(eventName, System.nanoTime()));
            return this;
        }

        public void end() {
            if (!finished) {
                this.endTimeNanos = System.nanoTime();
                this.finished = true;
                if (tracer != null) {
                    tracer.recordSpan(this);
                }
            }
        }

        public String getTraceId() { return traceId; }
        public String getParentSpanId() { return parentSpanId; }
        public String getSpanId() { return spanId; }
        public String getName() { return name; }
        public long getStartTimeNanos() { return startTimeNanos; }
        public long getEndTimeNanos() { return endTimeNanos; }
        public long getDurationNanos() { return (endTimeNanos > 0 ? endTimeNanos : System.nanoTime()) - startTimeNanos; }
        public double getDurationMs() { return getDurationNanos() / 1_000_000.0; }
        public Map<String, String> getAttributes() { return Collections.unmodifiableMap(attributes); }
        public List<SpanEvent> getEvents() { return Collections.unmodifiableList(events); }

        @Override
        public String toString() {
            return String.format("[%s] %s duration=%.2fms attrs=%s", traceId, name, getDurationMs(), attributes);
        }
    }

    public record SpanEvent(String name, long timestampNanos) {}
}
