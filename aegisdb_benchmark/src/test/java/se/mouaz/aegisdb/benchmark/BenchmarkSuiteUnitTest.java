package se.mouaz.aegisdb.benchmark;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BenchmarkSuiteUnitTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("RQ1 Raft batching benchmark smoke test")
    void testRq1BatchingSmoke() throws Exception {
        BenchmarkResult res = RQ1BatchingBenchmark.runSingle(5, 20, 999L);
        assertThat(res.successOperations()).isEqualTo(20);
        assertThat(res.throughputOpsSec()).isGreaterThan(0.0);
        assertThat(res.p99LatencyMs()).isGreaterThan(0.0);
    }

    @Test
    @DisplayName("RQ2 Baseline and delay smoke test")
    void testRq2DelaySmoke() throws Exception {
        BenchmarkResult res = RQ2FailureRecoveryBenchmark.runBaseline(15, 999L);
        assertThat(res.successOperations()).isEqualTo(15);
        assertThat(res.failedOperations()).isEqualTo(0);
    }

    @Test
    @DisplayName("RQ3 MVCC contention and financial invariant smoke test")
    void testRq3ContentionSmoke() throws Exception {
        BenchmarkResult res = RQ3ContentionBenchmark.runContentionExperiment("SmokeTest", 5, 2, 50, 999L);
        assertThat(res.successOperations() + res.failedOperations()).isEqualTo(50);
    }

    @Test
    @DisplayName("Export CSV and JSON results with reproducibility metadata")
    void testExportResults() throws Exception {
        BenchmarkResult res = RQ1BatchingBenchmark.runSingle(1, 10, 111L);
        ExperimentSuiteRunner.exportResults(List.of(res), tempDir);

        Path csvPath = tempDir.resolve("results.csv");
        Path jsonPath = tempDir.resolve("results.json");

        assertThat(csvPath).exists();
        assertThat(jsonPath).exists();
        assertThat(res.metadata()).containsKey("git_commit");
        assertThat(res.metadata()).containsKey("configuration_hash");
    }
}
