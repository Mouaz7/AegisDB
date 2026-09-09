package se.mouaz.aegisdb.benchmark;

import java.util.Map;

/**
 * Immutable measurement result from a benchmark experiment run,
 * with full reproducibility metadata per Master Project Plan §20.
 */
public record BenchmarkResult(
        BenchmarkConfig config,
        long totalOperations,
        long successOperations,
        long failedOperations,
        double elapsedSeconds,
        double throughputOpsSec,
        double p50LatencyMs,
        double p95LatencyMs,
        double p99LatencyMs,
        double maxLatencyMs,
        double abortRate,
        double recoveryTimeMs,
        Map<String, Object> metadata
) {
    public String toCsvRow() {
        return String.format("%s,%d,%d,%d,%d,%s,%s,%d,%d,%.2f,%.2f,%.3f,%.3f,%.3f,%.3f,%.4f,%.2f",
                config.experimentName(),
                config.clusterSize(),
                config.clientCount(),
                config.batchSize(),
                config.totalOperations(),
                config.workload(),
                config.failureScenario(),
                successOperations,
                failedOperations,
                elapsedSeconds,
                throughputOpsSec,
                p50LatencyMs,
                p95LatencyMs,
                p99LatencyMs,
                maxLatencyMs,
                abortRate,
                recoveryTimeMs);
    }

    public static String csvHeader() {
        return "experiment,cluster_size,clients,batch_size,total_ops,workload,failure_scenario," +
                "success_ops,failed_ops,elapsed_sec,throughput_ops_sec,p50_ms,p95_ms,p99_ms,max_ms,abort_rate,recovery_ms";
    }
}
