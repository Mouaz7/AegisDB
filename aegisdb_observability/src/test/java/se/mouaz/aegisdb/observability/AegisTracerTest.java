package se.mouaz.aegisdb.observability;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AegisTracerTest {

    private AegisTracer tracer;

    @BeforeEach
    void setUp() {
        tracer = new AegisTracer();
    }

    @Test
    void shouldRecordAndCompleteSpan() {
        AegisTracer.TraceSpan span = tracer.startSpan("raft-append");
        span.setAttribute("key", "test-key");
        span.setAttribute("term", "3");
        span.addEvent("replicated_to_majority");
        span.end();

        List<AegisTracer.TraceSpan> recent = tracer.getRecentSpans();
        assertThat(recent).hasSize(1);
        AegisTracer.TraceSpan recorded = recent.get(0);
        assertThat(recorded.getName()).isEqualTo("raft-append");
        assertThat(recorded.getAttributes()).containsEntry("key", "test-key");
        assertThat(recorded.getAttributes()).containsEntry("term", "3");
        assertThat(recorded.getEvents()).hasSize(1);
        assertThat(recorded.getEvents().get(0).name()).isEqualTo("replicated_to_majority");
        assertThat(recorded.getDurationMs()).isGreaterThanOrEqualTo(0.0);
    }

    @Test
    void shouldSupportParentChildRelationship() {
        AegisTracer.TraceSpan parent = tracer.startSpan("client-request");
        AegisTracer.TraceSpan child = tracer.startSpan(parent.getTraceId(), parent.getSpanId(), "query-router");
        child.end();
        parent.end();

        List<AegisTracer.TraceSpan> recent = tracer.getRecentSpans();
        assertThat(recent).hasSize(2);
        assertThat(recent.get(0).getParentSpanId()).isEqualTo(parent.getSpanId());
        assertThat(recent.get(0).getTraceId()).isEqualTo(parent.getTraceId());
    }
}
