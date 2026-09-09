package se.mouaz.aegisdb.benchmark;

import java.util.Collections;
import java.util.Map;

/**
 * Immutable configuration parameters for a benchmark experiment execution.
 */
public record BenchmarkConfig(
        String experimentName,
        int clusterSize,
        int clientCount,
        int batchSize,
        int totalOperations,
        long seed,
        String workload,
        String failureScenario,
        Map<String, String> additionalParams
) {
    public BenchmarkConfig {
        if (additionalParams == null) {
            additionalParams = Collections.emptyMap();
        }
    }

    public static Builder builder(String experimentName) {
        return new Builder(experimentName);
    }

    public static final class Builder {
        private final String experimentName;
        private int clusterSize = 3;
        private int clientCount = 1;
        private int batchSize = 1;
        private int totalOperations = 1000;
        private long seed = 42L;
        private String workload = "MIXED";
        private String failureScenario = "NONE";
        private Map<String, String> additionalParams = Collections.emptyMap();

        public Builder(String experimentName) {
            this.experimentName = experimentName;
        }

        public Builder clusterSize(int clusterSize) { this.clusterSize = clusterSize; return this; }
        public Builder clientCount(int clientCount) { this.clientCount = clientCount; return this; }
        public Builder batchSize(int batchSize) { this.batchSize = batchSize; return this; }
        public Builder totalOperations(int totalOperations) { this.totalOperations = totalOperations; return this; }
        public Builder seed(long seed) { this.seed = seed; return this; }
        public Builder workload(String workload) { this.workload = workload; return this; }
        public Builder failureScenario(String failureScenario) { this.failureScenario = failureScenario; return this; }
        public Builder additionalParams(Map<String, String> params) { this.additionalParams = params; return this; }

        public BenchmarkConfig build() {
            return new BenchmarkConfig(experimentName, clusterSize, clientCount, batchSize,
                    totalOperations, seed, workload, failureScenario, additionalParams);
        }
    }
}
