package se.mouaz.aegisdb.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Master automated research experiment suite runner.
 *
 * Conforms to Master Project Plan §17 (Phase 11), §20 (Research Evaluation), and §21:
 * - Executes RQ1: Raft batching vs throughput/latency
 * - Executes RQ2: Leader failure and network delay recovery
 * - Executes RQ3: MVCC transaction contention & abort rates
 * - Exports full results and reproducibility metadata to CSV and JSON.
 */
public class ExperimentSuiteRunner {
    private static final Logger log = LoggerFactory.getLogger(ExperimentSuiteRunner.class);

    public static void main(String[] args) throws Exception {
        System.out.println("=======================================================================");
        System.out.println("   AegisDB: Master Research Benchmark Suite (Phase 11)");
        System.out.println("   Evaluating Research Questions RQ1, RQ2, and RQ3");
        System.out.println("   Master Project Plan §15, §17, §20 & §21");
        System.out.println("=======================================================================\n");

        long seed = 12345L;
        List<BenchmarkResult> allResults = new ArrayList<>();

        // 1. Research Question 1: Raft Write Batching
        System.out.println("▶ [1/3] Executing RQ1 Experiments: Raft Batching (sizes 1, 10, 50, 100)...");
        List<BenchmarkResult> rq1Results = RQ1BatchingBenchmark.runSuite(200, seed);
        allResults.addAll(rq1Results);
        printTable("RQ1: Raft Write Batching Evaluation", rq1Results);

        // 2. Research Question 2: Failure Recovery & Delays
        System.out.println("\n▶ [2/3] Executing RQ2 Experiments: Fault Recovery & Availability...");
        List<BenchmarkResult> rq2Results = RQ2FailureRecoveryBenchmark.runSuite(100, seed);
        allResults.addAll(rq2Results);
        printTable("RQ2: Fault Recovery & Network Delays Evaluation", rq2Results);

        // 3. Research Question 3: MVCC Contention & Abort Rates
        System.out.println("\n▶ [3/3] Executing RQ3 Experiments: MVCC Contention & Invariant Preservation...");
        List<BenchmarkResult> rq3Results = RQ3ContentionBenchmark.runSuite(2000, seed);
        allResults.addAll(rq3Results);
        printTable("RQ3: MVCC Contention & Financial Invariant Evaluation", rq3Results);

        // 4. Export Artifacts
        exportResults(allResults, Paths.get("experiments", "data"));

        System.out.println("\n=======================================================================");
        System.out.println("   ✔ All Research Experiments Completed Successfully!");
        System.out.println("   Artifacts exported to: experiments/data/results.csv and results.json");
        System.out.println("=======================================================================");
    }

    public static void exportResults(List<BenchmarkResult> results, Path outputDir) throws Exception {
        Files.createDirectories(outputDir);

        // 1. Export CSV
        Path csvPath = outputDir.resolve("results.csv");
        try (PrintWriter pw = new PrintWriter(new FileWriter(csvPath.toFile()))) {
            pw.println(BenchmarkResult.csvHeader());
            for (BenchmarkResult res : results) {
                pw.println(res.toCsvRow());
            }
        }
        log.info("Exported CSV benchmark results to: {}", csvPath.toAbsolutePath());

        // 2. Export JSON with full metadata
        Path jsonPath = outputDir.resolve("results.json");
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(jsonPath.toFile(), results);
        log.info("Exported JSON benchmark results to: {}", jsonPath.toAbsolutePath());
    }

    private static void printTable(String title, List<BenchmarkResult> results) {
        System.out.println("---------------------------------------------------------------------------------------------------------");
        System.out.println("  " + title);
        System.out.println("---------------------------------------------------------------------------------------------------------");
        System.out.printf("%-24s | %-8s | %-12s | %-10s | %-10s | %-10s | %-10s%n",
                "Experiment", "Ops/Sec", "P50 Latency", "P95 Lat", "P99 Lat", "Abort Rate", "Recovery");
        System.out.println("---------------------------------------------------------------------------------------------------------");
        for (BenchmarkResult r : results) {
            System.out.printf("%-24s | %8.1f | %9.3f ms | %7.3f ms | %7.3f ms | %9.2f%% | %7.1f ms%n",
                    r.config().experimentName(),
                    r.throughputOpsSec(),
                    r.p50LatencyMs(),
                    r.p95LatencyMs(),
                    r.p99LatencyMs(),
                    r.abortRate() * 100.0,
                    r.recoveryTimeMs());
        }
        System.out.println("---------------------------------------------------------------------------------------------------------");
    }
}
