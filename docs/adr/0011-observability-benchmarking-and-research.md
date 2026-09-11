# ADR 0011: Observability, Benchmarking, and Research Evaluation

## Status
Accepted

## Context
Following the completion of Chaos Engineering and Security Hardening in Phase 10 (ADR 0010), AegisDB requires production-grade observability and an automated, reproducible research benchmarking harness per Master Project Plan §3, §4, §5, §11, §15, §17, §18, §20, §21, and §26 (Phase 11; US018, US019).

Key requirements:
1. **Non-Blocking Telemetry & Operational Metrics (US019)**:
   - Collection of metrics defined in §15 (`request_count`, `read_count`, `write_count`, `transaction_count`, `transaction_abort_count`, `leader_election_count`, `raft_log_size`, `replication_lag`, `wal_size`, `snapshot_count`, `p50_latency`, `p95_latency`, `p99_latency`, `throughput`).
   - Standard Prometheus OpenMetrics exposition format on the `/metrics` endpoint.
   - Tracing request execution spans (`client-route -> leader-append -> replication -> majority-commit -> mvcc-apply -> persistence -> response`).
   - Critical rule (§15): Telemetry failures must degrade visibility, never consensus safety or transaction correctness.
2. **Reproducible Research Evaluation (US018)**:
   - Addressing the core research questions:
     - **RQ1**: How does Raft write batching affect throughput and tail latency in a Java-based replicated database? (Batch sizes 1, 10, 50, 100).
     - **RQ2**: How do leader failures and network delays affect availability, recovery time, and P99 latency? (Baseline, network delays, leader kill failover).
     - **RQ3**: How does transaction contention affect latency and abort rate when using MVCC? (Contention scale, abort rates, financial invariant conservation).
   - Exporting structured data in CSV and JSON formats with comprehensive reproducibility metadata (`git_commit`, `java_version`, `seed`, `configuration_hash`, `timestamp`).
3. **Visual Dashboards & Tooling**:
   - Containerized Prometheus + Grafana stack via Docker Compose.
   - Pre-provisioned Grafana dashboard JSON visualizing cluster state, terms, throughput, latency percentiles, and abort rates.
   - Python visualization script generating publication-ready research graphs.
4. **Clean Architecture (§11)**:
   - Observability and benchmarking subsystems must reside in decoupled modules (`aegisdb_observability` and `aegisdb_benchmark`), leaving core consensus and storage unburdened by external monitoring frameworks.

## Decision
1. **Dedicated Modules**:
   - `aegisdb_observability`: Contains `AegisMetrics`, `AegisTracer`, and `AegisTelemetry`.
   - `aegisdb_benchmark`: Contains `BenchmarkConfig`, `BenchmarkResult`, `RQ1BatchingBenchmark`, `RQ2FailureRecoveryBenchmark`, `RQ3ContentionBenchmark`, and `ExperimentSuiteRunner`.
2. **High-Performance Concurrent Metrics**:
   - Used Java concurrency primitives (`LongAdder`, `AtomicLong`, `ConcurrentLinkedDeque`) to guarantee lock-free recording on hot paths.
   - Sliding-window percentile reservoir computes P50, P95, and P99 tail latencies on demand.
3. **Native OpenMetrics Text Formatting**:
   - Implemented zero-dependency Prometheus text generation within `AegisMetrics`, directly mounted on `/metrics` in `ManagementHttpServer`.
4. **Automated Research Runner & Plotting**:
   - `ExperimentSuiteRunner` automates execution across all parameter matrices, strictly asserts financial balance conservation invariants under contention, and dumps JSON/CSV datasets.
   - `scripts/plot_benchmarks.py` renders publication-ready PNG figures (`rq1_batching.png`, `rq2_recovery.png`, `rq3_contention.png`).

## Consequences
### Positive
- **Deterministic & Reproducible**: All research experiments are parameterized by deterministic seeds and hardware/JVM metadata.
- **Production Observability**: Prometheus can scrape cluster nodes without additional sidecars.
- **Zero Core Overhead**: If metrics or tracing fail or are omitted, Raft consensus and ACID transactions remain unaffected.
- **Complete Master Plan Compliance**: All requirements of Phase 11 and research evaluation (§20) are fulfilled.

### Negative / Trade-offs
- In-memory percentile tracking retains recent latency samples up to a configured window bound (e.g. 5,000 samples) to prevent unbounded memory growth.
