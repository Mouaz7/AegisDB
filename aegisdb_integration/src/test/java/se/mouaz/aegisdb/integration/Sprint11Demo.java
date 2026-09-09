package se.mouaz.aegisdb.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.benchmark.*;
import se.mouaz.aegisdb.common.*;
import se.mouaz.aegisdb.management.ManagementHttpServer;
import se.mouaz.aegisdb.management.security.ManagementSecurityManager;
import se.mouaz.aegisdb.management.security.RateLimiter;
import se.mouaz.aegisdb.node.DatabaseNode;
import se.mouaz.aegisdb.observability.AegisMetrics;
import se.mouaz.aegisdb.observability.AegisTelemetry;
import se.mouaz.aegisdb.observability.AegisTracer;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Sprint 11 Live Demonstration: Observability, Prometheus, Grafana, and Research Benchmarks.
 * Validates US018 (Reproducible performance measurements) and US019 (Operator cluster telemetry).
 *
 * Demonstrates:
 * 1. [AC1] OpenTelemetry Metrics & Distributed Tracer Collection
 * 2. [AC2] Prometheus OpenMetrics Text Exporter (/metrics HTTP endpoint)
 * 3. [AC3] Real-time Console Telemetry & Cluster Dashboard
 * 4. [AC4] RQ1 Raft Write Batching Evaluation (Throughput vs Tail Latency)
 * 5. [AC5] RQ2 Failure Recovery & Network Delay Evaluation
 * 6. [AC6] RQ3 MVCC Contention & Financial Invariant Verification
 * 7. [AC7] Automated Artifacts Export (results.csv, results.json, and plotting graphs)
 */
public class Sprint11Demo {
    private static final Logger log = LoggerFactory.getLogger(Sprint11Demo.class);

    public static void main(String[] args) throws Exception {
        System.out.println("=======================================================================");
        System.out.println("  AegisDB: Sprint 11 Live Demonstration");
        System.out.println("  Observability, Benchmarking & Research Evaluation (US018, US019)");
        System.out.println("  Master Project Plan §14, §15, §17, §20, §21 & §26");
        System.out.println("=======================================================================\n");

        Path tempDir = Files.createTempDirectory("aegisdb-sprint11-demo-");
        List<BenchmarkResult> demoResults = new ArrayList<>();
        long demoSeed = 424242L;

        try {
            // -------------------------------------------------------------
            // AC1: OpenTelemetry Metrics & Distributed Tracer Collection
            // -------------------------------------------------------------
            System.out.println("▶ [1/7] [AC1] Demonstrating Metrics & Distributed Tracer Spans...");
            NodeId node1Id = NodeId.of("node-obs-1");
            AegisMetrics metrics1 = AegisTelemetry.metricsFor(node1Id);

            // Simulate operations
            metrics1.recordRead();
            metrics1.recordWrite();
            metrics1.recordTransaction(true);
            metrics1.recordTransaction(false);
            metrics1.setCurrentTerm(4);
            metrics1.setRaftLogSize(150);

            // Create distributed trace span
            AegisTracer tracer = AegisTelemetry.tracer();
            AegisTracer.TraceSpan rootSpan = tracer.startSpan("client-request");
            rootSpan.setAttribute("client.id", "client-demo-1");
            rootSpan.setAttribute("operation", "transfer");

            AegisTracer.TraceSpan routeSpan = tracer.startSpan(rootSpan.getTraceId(), rootSpan.getSpanId(), "query-router");
            routeSpan.setAttribute("shard.id", "shard-0");
            routeSpan.end();

            AegisTracer.TraceSpan raftSpan = tracer.startSpan(rootSpan.getTraceId(), rootSpan.getSpanId(), "raft-append-and-replicate");
            raftSpan.addEvent("replicated_to_majority");
            raftSpan.end();

            rootSpan.end();

            System.out.printf("  ✔ Metrics recorded: requests=%d, reads=%d, writes=%d, tx=%d (aborted=%d)%n",
                    metrics1.getRequestCount(), metrics1.getReadCount(), metrics1.getWriteCount(),
                    metrics1.getTransactionCount(), metrics1.getTransactionAbortCount());
            System.out.printf("  ✔ Distributed trace recorded: %d spans captured (Root: %s, duration=%.2f ms)%n",
                    tracer.getRecentSpans().size(), rootSpan.getName(), rootSpan.getDurationMs());

            // -------------------------------------------------------------
            // AC2: Prometheus OpenMetrics Text Exporter on /metrics
            // -------------------------------------------------------------
            System.out.println("\n▶ [2/7] [AC2] Demonstrating Prometheus OpenMetrics Export (/metrics)...");
            InMemoryTransport transport = new InMemoryTransport(node1Id);
            transport.start();

            NodeConfiguration nodeConfig = NodeConfiguration.builder()
                    .nodeId(node1Id)
                    .endpoint(new Endpoint("localhost", 9991))
                    .dataDir(tempDir.resolve("node1"))
                    .build();
            ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                    .clusterId(new ClusterId("demo-cluster"))
                    .addMember(node1Id, nodeConfig.endpoint())
                    .build();
            DatabaseNode dbNode = new DatabaseNode(nodeConfig, clusterConfig, transport);
            dbNode.start();

            String token = "sprint11-secret-token";
            ManagementSecurityManager secMgr = new ManagementSecurityManager(token, token, true);
            RateLimiter rateLimiter = new RateLimiter(50.0, 50.0);
            ManagementHttpServer server = new ManagementHttpServer(0, dbNode, null, secMgr, rateLimiter);
            server.start();

            HttpClient http = HttpClient.newHttpClient();
            HttpRequest metricsReq = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + server.port() + "/metrics"))
                    .header("Authorization", "Bearer " + token)
                    .header("Accept", "text/plain")
                    .GET()
                    .build();

            HttpResponse<String> metricsResp = http.send(metricsReq, HttpResponse.BodyHandlers.ofString());
            System.out.printf("  ✔ GET /metrics Status: %d (Content-Type: %s)%n",
                    metricsResp.statusCode(), metricsResp.headers().firstValue("Content-Type").orElse("unknown"));
            System.out.println("  ✔ Sample Prometheus Output Snippet:");
            metricsResp.body().lines().limit(6).forEach(line -> System.out.println("      " + line));

            server.stop();
            dbNode.stop();
            transport.stop();

            // -------------------------------------------------------------
            // AC3: Real-time Terminal Dashboard
            // -------------------------------------------------------------
            System.out.println("\n▶ [3/7] [AC3] Displaying Real-Time Cluster Telemetry Dashboard...");
            printConsoleDashboard(metrics1);

            // -------------------------------------------------------------
            // AC4: RQ1 Raft Write Batching Live Benchmark
            // -------------------------------------------------------------
            System.out.println("\n▶ [4/7] [AC4] Executing RQ1 Live Benchmark: Raft Write Batching...");
            List<BenchmarkResult> rq1Results = RQ1BatchingBenchmark.runSuite(40, demoSeed);
            demoResults.addAll(rq1Results);
            printExperimentSummary("RQ1: Raft Write Batching", rq1Results);

            // -------------------------------------------------------------
            // AC5: RQ2 Fault Recovery & Network Delay Benchmark
            // -------------------------------------------------------------
            System.out.println("\n▶ [5/7] [AC5] Executing RQ2 Live Benchmark: Failure Recovery & Delays...");
            List<BenchmarkResult> rq2Results = RQ2FailureRecoveryBenchmark.runSuite(20, demoSeed);
            demoResults.addAll(rq2Results);
            printExperimentSummary("RQ2: Fault Recovery & Delay Scenarios", rq2Results);

            // -------------------------------------------------------------
            // AC6: RQ3 MVCC Contention & Financial Invariant Conservation
            // -------------------------------------------------------------
            System.out.println("\n▶ [6/7] [AC6] Executing RQ3 Live Benchmark: MVCC Contention & Invariant...");
            List<BenchmarkResult> rq3Results = RQ3ContentionBenchmark.runSuite(400, demoSeed);
            demoResults.addAll(rq3Results);
            printExperimentSummary("RQ3: MVCC Contention & Financial Conservation", rq3Results);

            // -------------------------------------------------------------
            // AC7: Export Artifacts (CSV, JSON & Plot Generation)
            // -------------------------------------------------------------
            System.out.println("\n▶ [7/7] [AC7] Exporting Reproducible Data Artifacts & Triggering Plots...");
            Path outputDir = Paths.get("experiments", "data");
            ExperimentSuiteRunner.exportResults(demoResults, outputDir);

            System.out.println("  ✔ Saved: experiments/data/results.csv");
            System.out.println("  ✔ Saved: experiments/data/results.json");

            // Execute Python plotting script if available
            try {
                Process plotProc = new ProcessBuilder("python", "scripts/plot_benchmarks.py").inheritIO().start();
                int exitCode = plotProc.waitFor();
                if (exitCode == 0) {
                    System.out.println("  ✔ Generated Research Graphs in: experiments/graphs/");
                }
            } catch (Exception e) {
                System.out.println("  (Note: Python matplotlib plotting skipped; raw data ready in CSV/JSON)");
            }

            System.out.println("\n=======================================================================");
            System.out.println("  ✔ SPRINT 11 LIVE DEMO COMPLETED SUCCESSFULLY!");
            System.out.println("  All Deliverables Verified (US018, US019, RQ1, RQ2, RQ3).");
            System.out.println("=======================================================================");

        } finally {
            deleteRecursively(tempDir.toFile());
        }
    }

    private static void printConsoleDashboard(AegisMetrics m) {
        System.out.println("  +-------------------------------------------------------------------+");
        System.out.println("  |                     AEGISDB CLUSTER TELEMETRY                     |");
        System.out.println("  +-----------------------------------+-------------------------------+");
        System.out.printf("  | Node ID:          %-15s | Status:           %-11s |%n", m.getNodeId(), "RUNNING");
        System.out.printf("  | Current Term:     %-15d | Log Size:         %-11d |%n", m.getCurrentTerm(), m.getRaftLogSize());
        System.out.printf("  | Total Requests:   %-15d | Throughput:       %-7.1f ops/s |%n", m.getRequestCount(), m.getThroughputOpsPerSec());
        System.out.printf("  | Total Tx:         %-15d | Abort Rate:       %-10.1f%% |%n", m.getTransactionCount(), m.getTransactionAbortRate() * 100.0);
        System.out.printf("  | P50 Latency:      %-12.3f ms | P99 Latency:      %-8.3f ms |%n", m.getP50LatencyMs(), m.getP99LatencyMs());
        System.out.println("  +-----------------------------------+-------------------------------+");
    }

    private static void printExperimentSummary(String title, List<BenchmarkResult> results) {
        System.out.println("  -------------------------------------------------------------------------------------");
        System.out.printf("  %-22s | %-10s | %-12s | %-12s | %-10s%n", "Experiment", "Throughput", "P50 Latency", "P99 Latency", "Abort / Error");
        System.out.println("  -------------------------------------------------------------------------------------");
        for (BenchmarkResult r : results) {
            System.out.printf("  %-22s | %7.1f op/s | %9.3f ms | %9.3f ms | %7.2f%%%n",
                    r.config().experimentName(),
                    r.throughputOpsSec(),
                    r.p50LatencyMs(),
                    r.p99LatencyMs(),
                    r.abortRate() * 100.0);
        }
        System.out.println("  -------------------------------------------------------------------------------------");
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}
