# AegisDB: Research Benchmarks & Empirical Evaluation

This document outlines the reproducible research evaluation methodology, research questions, and measurement results for **AegisDB**, conforming to **Master Project Plan §14, §15, §20 & §21 (Sprint 11)**.

---

## 1. Research Questions & Hypotheses

### RQ1: Raft Write Batching vs Throughput & Tail Latency
> **Question:** *How does Raft write batching affect throughput and tail latency in a Java-based replicated database?*

- **Hypothesis:** Batching multiple log entries into fewer RPC round-trips and fsync operations amortizes fixed coordination overhead, yielding superlinear throughput scaling up to network bandwidth saturation, while increasing mean P50 and P99 tail latencies linearly with batch accumulation windows.
- **Evaluation Matrix:**
  - Batch sizes: $B \in \{1, 10, 50, 100\}$.
  - Cluster configuration: 3-node in-memory and gRPC replicated cluster.
  - Metrics: Throughput (ops/sec), P50 latency (ms), P95 latency (ms), P99 latency (ms).

### RQ2: Fault Recovery & Network Delay Resilience
> **Question:** *How do leader failures and network delays affect availability, recovery time, and P99 latency?*

- **Hypothesis:** Under adverse network conditions (injected latency jitter, packet loss), consensus availability is maintained as long as a quorum of peers can communicate. Leader crashes induce a transient failover latency spike bounded by randomized election timeouts ($T_{election} \in [150\text{ms}, 300\text{ms}]$), after which tail latency recovers to baseline without uncommitted state loss.
- **Evaluation Matrix:**
  - Scenarios: Steady-state baseline, 15ms injected RPC delay, mid-flight leader kill failover.
  - Metrics: Availability (success rate), recovery duration (ms), tail latency (P99 ms), throughput.

### RQ3: MVCC Contention & Abort Rates
> **Question:** *How does transaction contention affect latency and abort rate when using MVCC?*

- **Hypothesis:** In multi-version concurrency control (MVCC) with first-committer-wins conflict resolution, transaction abort rates correlate strongly with access skew and write-set overlap. Strict serializability is preserved under all contention levels, and financial conservation invariants ($\sum A_i = C$) are never violated.
- **Evaluation Matrix:**
  - Scenarios: Low Contention (100 distinct keys), Moderate Contention (20 accounts), High Contention (5 hot accounts).
  - Invariant: Financial conservation strictly verified ($A + B + C... = 50,000$, $\Delta = 0$).
  - Metrics: Abort rate (%), throughput (tx/sec), P50/P95/P99 latency (ms).

---

## 2. Reproducibility Metadata

Per Master Project Plan §20, every experiment export captures full execution provenance:
- `git_commit`: Exact Git commit SHA hash.
- `java_version` & `java_vendor`: JVM version details.
- `os_name` & `os_arch`: Host operating system and CPU architecture.
- `available_processors`: CPU core concurrency bounds.
- `seed`: Deterministic pseudo-random seed used for fault injection and client random access.
- `configuration_hash`: SHA-256 fingerprint of the experiment parameters.
- `timestamp`: UTC execution timestamp.

---

## 3. How to Run & Visualize

### Running the Full Experiment Suite
```bash
# Execute master research benchmark harness
mvn exec:java -pl aegisdb_benchmark \
    -Dexec.mainClass=se.mouaz.aegisdb.benchmark.ExperimentSuiteRunner
```

### Running the Live Interactive Demonstration
```bash
./scripts/run-sprint11-demo.sh
```

### Generating Publication Graphs
```bash
python scripts/plot_benchmarks.py
```
Generated research figures are placed in `experiments/graphs/`:
- `rq1_batching.png`
- `rq2_recovery.png`
- `rq3_contention.png`

---

## 4. Telemetry Stack (Prometheus & Grafana)

AegisDB nodes natively expose standard Prometheus text exposition format on `/metrics`.

To launch the containerized monitoring stack:
```bash
cd docker/
docker compose up -d
```
- **Prometheus**: `http://localhost:9090` (scrapes AegisDB at `/metrics`)
- **Grafana**: `http://localhost:3000` (credentials: `admin` / `aegisdb`, pre-provisioned AegisDB dashboard)
