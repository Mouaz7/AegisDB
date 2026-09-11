package se.mouaz.aegisdb.observability;

import se.mouaz.aegisdb.common.NodeId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Unified facade managing metrics, tracing, and operational telemetry
 * for AegisDB clusters and individual nodes.
 */
public final class AegisTelemetry {

    private static final Map<String, AegisMetrics> NODE_METRICS = new ConcurrentHashMap<>();
    private static final AegisTracer TRACER = AegisTracer.global();

    private AegisTelemetry() {}

    public static AegisMetrics metricsFor(NodeId nodeId) {
        return NODE_METRICS.computeIfAbsent(nodeId.value(), AegisMetrics::new);
    }

    public static AegisMetrics metricsFor(String nodeId) {
        return NODE_METRICS.computeIfAbsent(nodeId, AegisMetrics::new);
    }

    public static AegisTracer tracer() {
        return TRACER;
    }

    public static String exportAllPrometheusMetrics() {
        StringBuilder sb = new StringBuilder(4096);
        for (AegisMetrics metrics : NODE_METRICS.values()) {
            sb.append(metrics.exportPrometheusText()).append("\n");
        }
        if (NODE_METRICS.isEmpty()) {
            sb.append(AegisMetrics.global().exportPrometheusText());
        }
        return sb.toString();
    }

    public static void clear() {
        NODE_METRICS.clear();
        TRACER.clear();
    }
}
