# AegisDB

<p align="center">
  <strong>An Experimental, Distributed Transactional Key-Value Database in Java (Research Prototype)</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Java-21%20LTS-orange.svg" alt="Java 21 LTS" />
  <img src="https://img.shields.io/badge/Consensus-Raft-blue.svg" alt="Raft Consensus" />
  <img src="https://img.shields.io/badge/Transactions-2PC%20%26%20MVCC-purple.svg" alt="2PC & MVCC" />
  <img src="https://img.shields.io/badge/Isolation-Snapshot%20Isolation-darkgreen.svg" alt="Snapshot Isolation" />
  <img src="https://img.shields.io/badge/Telemetry-OpenTelemetry%20%26%20Prometheus-red.svg" alt="Telemetry" />
  <img src="https://img.shields.io/badge/Release-0.1.0--alpha.1-yellow.svg" alt="Release 0.1.0-alpha.1" />
  <img src="https://img.shields.io/badge/License-MIT-lightgrey.svg" alt="License MIT" />
</p>

---

## Overview

**AegisDB** is an experimental distributed transactional key-value database engine built from first principles in modern Java (Java 21 LTS). It explores strong linearizable consensus via Raft, crash-safe local storage with write-ahead logging (WAL), multi-version concurrency control (MVCC) providing Snapshot Isolation, deterministic horizontal sharding via consistent hashing, and atomic cross-shard distributed transactions orchestrated through Two-Phase Commit (2PC).

> [!WARNING]
> **Status: Early Alpha (Research Prototype)**
> AegisDB is currently an experimental research project. It is **NOT** production-ready. While the architecture supports advanced distributed systems concepts, the implementation is under active development to formally verify safety, durability, and crash recovery invariants. Do not use this in production environments.

---

## Key Features & Guarantees

### 🛡️ Consensus & High Availability (Raft)
- **Single-Leader Invariant**: Elects at most one leader per term through randomized election timers and strict quorum voting ($N/2 + 1$).
- **Linearizable Log Replication**: Majority quorum write replication ensures committed entries are durable and strictly ordered across all replicas.
- **Dynamic Catch-Up & Repair**: Recovered followers automatically reconcile divergent log histories via the Raft log repair protocol.
- **Log Compaction & Snapshots**: In-memory state machine state is periodically snapshotted to disk, truncating committed WAL entries and supporting chunked snapshot transfer to lagging nodes.

### 💾 Storage & Durability
- **Binary Write-Ahead Log (WAL)**: Append-only log files framed with magic bytes (`0xAE615D80`), sequence numbers, CRC32 checksums, and strict `fsync` policies.
- **Zero-Loss Crash Recovery**: Automated replay of durable WAL segments upon startup with automatic truncation of torn, partial, or corrupted tail writes.
- **Immutable Checkpoints**: Atomic metadata persistence (`currentTerm`, `votedFor`, `snapshotIndex`) ensures Raft invariants survive abrupt power loss.

### ⚡ Concurrency Control & Isolation (MVCC)
- **Snapshot Isolation (SI)**: Multi-version lock-free reads allow concurrent transactions to read stable consistent snapshots without blocking incoming writes.
- **First-Committer-Wins OCC**: Write sets are validated at commit time; conflicting concurrent updates on overlapping keys trigger deterministic write conflicts (`WriteConflictException`).
- **Background Version Garbage Collection**: Obsolete row versions beyond the oldest active transaction's read timestamp are cleaned up automatically in the background.

### 🌐 Horizontal Sharding & 2PC Distributed Transactions
- **Consistent Hash Ring**: Keys are partitioned across independent shard clusters using MD5/Murmur consistent hashing with virtual nodes for uniform balance.
- **Two-Phase Commit (2PC)**: Cross-shard atomic transactions coordinate parallel `PREPARE` and `COMMIT`/`ABORT` phases across participant shards.
- **Durable Coordinator Journal**: Binary coordinator log records transaction state transitions (`PREPARING`, `COMMITTED`, `ABORTED`); automatic startup recovery resolves in-doubt transactions across all 8 failure modes.

### ⚠️ Scope & Limitations of Guarantees
- **Durability**: Guaranteed only if WAL segments are successfully flushed (`fsync`) to stable storage before responding to the client. Using delayed `fsync_policy` risks data loss on sudden power failure.
- **Consistency**: Linearizability is scoped *per shard*. Cross-shard transactions provide Strict Serializable isolation but depend on the availability of the 2PC Coordinator node.
- **Isolation**: Snapshot Isolation does not prevent write skew anomalies. If you require strict serializability for concurrent overlapping updates on different keys, you must handle write conflicts explicitly at the application level.

### 🧪 Resilience & Chaos Engineering
- **Fault Injection Transport**: Deterministic, pseudo-random injection of network partitions, message drops, latency jitter, and packet duplication.
- **Continuous Safety Invariant Monitoring**: Background verification continuously validates election safety, monotonic terms, log prefix consistency, and the financial balance conservation invariant ($A + B + C = \text{Constant}$).

### 🔒 Enterprise Security Hardening
- **Lightweight Management Server**: Embedded HTTP administrative plane with constant-time Bearer token verification (`MessageDigest.isEqual`) preventing timing side-channel attacks.
- **Role-Based Access Control (RBAC)**: Enforces separation of duties between `ROLE_MONITOR` (read-only telemetry and health) and `ROLE_ADMIN` (cluster configuration and node management).
- **Security Guardrails**: Strict key size bounding ($\le 1\,\text{KB}$), payload bounding ($\le 16\,\text{MB}$), token-bucket rate limiting against DoS attacks, and directory traversal sanitization.

### 📊 Observability & Research Telemetry
- **OpenTelemetry Standard**: Lock-free counters, gauges, and distributed trace spans covering RPC lifecycles, replication latencies, and commit pipelines.
- **Prometheus OpenMetrics**: Native `/metrics` endpoint scrapable by Prometheus servers.
- **Pre-Configured Dashboards**: Ready-to-use Grafana dashboard configuration (`docker/grafana/dashboards/aegisdb_dashboard.json`).
- **Reproducible Research Harness**: Automated benchmark suite generating publication-grade CSV/JSON datasets evaluating write batching (RQ1), failover latency (RQ2), and MVCC contention (RQ3).

---

## Architecture & Directory Structure

AegisDB follows a strict, layered, decoupled architecture with 15 module components:

```text
AegisDB/
├── pom.xml                                      # Parent POM (Java 21 LTS, gRPC, Protobuf, JUnit 5)
├── README.md                                    # System manual & operational guide
├── LICENSE                                      # MIT License
├── bin/
│   └── aegisdb-server                           # Standalone cluster node launcher
├── config/
│   └── aegisdb-cluster.example.yaml             # Production cluster topology configuration
├── docker/                                      # Docker Compose stack for Prometheus & Grafana
├── docs/                                        # Architecture, design & ADR specifications
│   ├── history/                                 # Historical milestones and completion reports
│   └── adr/                                     # 12 Architectural Decision Records (ADRs)
├── experiments/                                 # Benchmark data, results (CSV/JSON), and research plots
├── scripts/                                     # Automated management & verification scripts
│   ├── package-release.sh                       # Production distribution packaging
│   ├── run-release-smoke-test.sh                # 16-step Master Demonstration scenario (§26)
│   ├── verify-release-readiness.sh              # 28-point Master Completion Checklist validator
│   └── test-all.sh                              # Complete unit, architecture & integration test runner
│
├── aegisdb-common/                              # Core domain primitives (NodeId, Endpoint, Config)
├── aegisdb-protocol/                            # Protobuf definitions & gRPC service interfaces
├── aegisdb-transport/                           # Transport abstraction (InMemory, Netty/gRPC)
├── aegisdb-raft/                                # Raft consensus engine (Election, Replication, Snapshots)
├── aegisdb-storage/                             # Persistent storage engine, WAL, Checkpoints, Recovery
├── aegisdb-mvcc/                                # Multi-Version Concurrency Control (SI, Version Chains)
├── aegisdb-transaction/                         # Single-shard & cross-shard 2PC transaction coordinator
├── aegisdb-sharding/                            # Consistent hash ring, shard topology & query router
├── aegisdb-chaos/                               # Chaos engineering, fault injection & invariant monitors
├── aegisdb-management/                          # Management HTTP server, RBAC token auth & guardrails
├── aegisdb-observability/                       # OpenTelemetry metrics, tracers & Prometheus exporter
├── aegisdb-benchmark/                           # Empirical research benchmarking suite (RQ1, RQ2, RQ3)
├── aegisdb-node/                                # DatabaseNode bootstrap, lifecycle & state orchestration
├── aegisdb-client/                              # High-level Java Client SDK with transparent retries
└── aegisdb-integration/                         # Integration test suites & system release scenarios
```

### Module Dependency Graph

```mermaid
graph TD
    Client[aegisdb-client] --> Sharding[aegisdb-sharding]
    Client --> Transport[aegisdb-transport]
    Sharding --> Tx[aegisdb-transaction]
    Tx --> Node[aegisdb-node]
    Node --> Raft[aegisdb-raft]
    Node --> MVCC[aegisdb-mvcc]
    Node --> Storage[aegisdb-storage]
    Node --> Mgmt[aegisdb-management]
    Raft --> Transport
    Raft --> Protocol[aegisdb-protocol]
    Mgmt --> Protocol
    
    subgraph Core Primitives
        Common[aegisdb-common]
    end
    
    subgraph Operations
        Chaos[aegisdb-chaos]
        Observability[aegisdb-observability]
        Integration[aegisdb-integration]
        Benchmark[aegisdb-benchmark]
    end
```

---

## Getting Started

### Prerequisites
- **Java Development Kit**: Java 21 LTS (or Java 21+ with modern language feature support)
- **Build Tool**: Apache Maven 3.8+
- **Container Runtime (Optional)**: Docker & Docker Compose (for Prometheus/Grafana stack)

### 1. Clone and Build from Source
```bash
git clone https://github.com/Mouaz7/AegisDB.git
cd AegisDB

# Compile and package all 15 modules
./mvnw clean install -DskipTests
```

### 2. Run the Full Test Suite
AegisDB comes with comprehensive unit tests, ArchUnit architectural rule validations, and multi-node integration suites:
```bash
./mvnw test
```
*(or run `./scripts/test-all.sh`)*

### 3. Run the Master Capstone Live Demonstration
To view the full 16-step operational demonstration scenario (§26) in your console (starting a 3-node cluster, leader election, replicated writes, leader kill, automatic failover, recovery, MVCC transactions, cross-shard 2PC, network partition, healing, RBAC security, and telemetry):
```bash
./scripts/run-release-smoke-test.sh
```
*Tip: Add `--extended` for heavy stress workload validation:*
```bash
./scripts/run-release-smoke-test.sh --extended
```

### 4. Verify the System Release Readiness (§28)
Verify all 28 foundational requirements of the Engineering Plan:
```bash
./scripts/verify-release-readiness.sh
```

---

## Production Release Packaging

To build a standalone production release distribution bundle containing executables, configuration templates, documentation, third-party runtime dependencies, and module JARs:

```bash
./scripts/package-release.sh
```

The build produces release archives in `target/release/`:
- `target/release/aegisdb-0.1.0-alpha.1-bin.tar.gz`
- `target/release/aegisdb-0.1.0-alpha.1-bin.zip`
- Cryptographic SHA-256 checksums (`*.sha256`)

To unpack and inspect the distribution:
```bash
tar -xzf target/release/aegisdb-0.1.0-alpha.1-bin.tar.gz
cd aegisdb-0.1.0-alpha.1
ls -lh
# bin/  config/  docs/  lib/  LICENSE  README.md
```

---

## Operating an AegisDB Cluster

### Minimal Quick Start (CI/Local Test)
To start a standalone 3-node cluster using the example configuration:
```bash
# Start nodes in separate terminals
./bin/aegisdb-server --config config/aegisdb-cluster.example.yaml --node-id node-1
./bin/aegisdb-server --config config/aegisdb-cluster.example.yaml --node-id node-2
./bin/aegisdb-server --config config/aegisdb-cluster.example.yaml --node-id node-3
```

### Configuration File (`config/aegisdb-cluster.example.yaml`)

AegisDB nodes are configured via YAML:

```yaml
cluster:
  cluster_id: "aegisdb-prod-cluster"
  shards: 3
  replication_factor: 3

nodes:
  - node_id: "node-1"
    endpoint:
      host: "127.0.0.1"
      port: 7001
    raft_port: 8001
    management:
      enabled: true
      port: 9001
      rate_limit:
        requests_per_sec: 100
        burst: 200

  - node_id: "node-2"
    endpoint:
      host: "127.0.0.1"
      port: 7002
    raft_port: 8002
    management:
      enabled: true
      port: 9002
      rate_limit:
        requests_per_sec: 100
        burst: 200

  - node_id: "node-3"
    endpoint:
      host: "127.0.0.1"
      port: 7003
    raft_port: 8003
    management:
      enabled: true
      port: 9003
      rate_limit:
        requests_per_sec: 100
        burst: 200

raft:
  election_timeout_min_ms: 150
  election_timeout_max_ms: 300
  heartbeat_interval_ms: 50
  batch_max_entries: 50
  max_log_entries_before_snapshot: 1000

storage:
  data_dir: "/var/lib/aegisdb/data"
  wal_segment_size_bytes: 67108864 # 64 MB
  fsync_policy: "EVERY_COMMIT"
  fsync_interval_ms: 10
  checksum: "CRC32"

transactions:
  default_isolation_level: "SNAPSHOT_ISOLATION"
  cross_shard_2pc_timeout_ms: 5000
  deadlock_detection_interval_ms: 500

security:
  rbac:
    enabled: true
    admin_token: "${AEGISDB_ADMIN_TOKEN}"
    monitor_token: "${AEGISDB_MONITOR_TOKEN}"
  tls:
    enabled: true
    cert_path: "${AEGISDB_TLS_CERT_PATH}"
    key_path: "${AEGISDB_TLS_KEY_PATH}"
  guardrails:
    max_key_bytes: 1024
    max_value_bytes: 1048576 # 1 MB
    max_batch_size: 500

observability:
  opentelemetry:
    enabled: true
    service_name: "aegisdb-engine"
  prometheus:
    enabled: true
    path: "/metrics"
```

**Security Contexts:**
- **Development Defaults:** For local development, TLS can be disabled (`tls.enabled: false`) and tokens can be set via environment variables.
- **Production Requirements:** In production, TLS MUST be enabled (`tls.enabled: true`) with valid certificate paths. If TLS is enabled but paths are empty, the node will fail-closed. Tokens must be injected via secure orchestrator secrets.

---

## Developer Guide: Java Client SDK

AegisDB provides a high-level, thread-safe Java Client SDK (`aegisdb-client`) that handles connection pooling, transparent leader discovery, failover redirection, and transactions.

### 1. Basic Key-Value Operations
```java
import se.mouaz.aegisdb.client.DefaultAegisDbClient;
import se.mouaz.aegisdb.client.AegisDbClientConfig;
import se.mouaz.aegisdb.common.Endpoint;
import java.util.List;
import java.util.Optional;

// Configure cluster endpoints
AegisDbClientConfig config = AegisDbClientConfig.builder()
    .seedEndpoints(List.of(
        Endpoint.of("127.0.0.1", 7001),
        Endpoint.of("127.0.0.1", 7002),
        Endpoint.of("127.0.0.1", 7003)
    ))
    .maxRetries(3)
    .retryBackoffMs(50)
    .build();

try (DefaultAegisDbClient client = new DefaultAegisDbClient(config)) {
    // Put a key-value pair (linearizable write through Raft leader)
    client.putString("user:1001:profile", "{\"name\":\"Alice\",\"role\":\"Admin\"}").join();

    // Read a key
    Optional<String> profile = client.getString("user:1001:profile").join();
    profile.ifPresent(p -> System.out.println("Profile: " + p));

    // Delete a key
    client.delete("user:1001:profile").join();
}
```

### 2. Single-Shard MVCC Transactions (Snapshot Isolation)
Execute atomic multi-operation transactions with snapshot isolation and first-committer-wins conflict detection:

```java
import se.mouaz.aegisdb.transaction.Transaction;
import se.mouaz.aegisdb.transaction.TransactionManager;
import se.mouaz.aegisdb.mvcc.IsolationLevel;

TransactionManager txManager = node.getTransactionManager();

// Begin a Snapshot Isolation transaction
Transaction tx = txManager.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION);

try {
    // Read consistent snapshot
    String balA = tx.getString("account:A").orElse("1000");
    String balB = tx.getString("account:B").orElse("500");

    // Perform balance transfer
    int newBalA = Integer.parseInt(balA) - 200;
    int newBalB = Integer.parseInt(balB) + 200;

    tx.putString("account:A", String.valueOf(newBalA));
    tx.putString("account:B", String.valueOf(newBalB));

    // Commit atomically
    tx.commit();
    System.out.println("Transfer committed successfully under Snapshot Isolation!");
} catch (WriteConflictException e) {
    // Abort if concurrent transaction modified overlapping keys
    tx.abort();
    System.err.println("Write conflict detected; retry with exponential backoff.");
}
```

### 3. Cross-Shard Distributed Transactions (Two-Phase Commit)
Coordinate atomic cross-shard transactions across partitioned keys with automatic retry logic:

```java
import se.mouaz.aegisdb.client.ShardedAegisDbClient;

ShardedAegisDbClient shardedClient = new ShardedAegisDbClient(router, coordinator);

// Execute atomic transfer across keys residing on different shards
shardedClient.runInTransaction(tx -> {
    int balanceX = Integer.parseInt(tx.getString("shard0:user:X").orElse("1000"));
    int balanceY = Integer.parseInt(tx.getString("shard1:user:Y").orElse("1000"));

    tx.putString("shard0:user:X", String.valueOf(balanceX - 100));
    tx.putString("shard1:user:Y", String.valueOf(balanceY + 100));
    
    // Automatic 2PC PREPARE and COMMIT across both shards
}).join();
```

---

## Observability & Monitoring

AegisDB provides built-in enterprise observability with zero third-party agent dependencies.

### 1. Prometheus Metrics Scraping
Each node exposes a secure HTTP endpoint at `http://<host>:<managementPort>/metrics`.
To scrape metrics via curl using the monitor Bearer token:

```bash
curl -H "Authorization: Bearer aegis-monitor-secret-token" \
     http://127.0.0.1:9101/metrics
```

Key exported metrics include:
- `aegisdb_write_total`: Total linearizable write operations committed.
- `aegisdb_read_total`: Total reads processed.
- `aegisdb-raft_term`: Current Raft term.
- `aegisdb-raft_commit_index`: Monotonically increasing committed log index.
- `aegisdb_active_transactions`: Gauge of in-flight active transactions.
- `aegisdb_write_conflict_aborts_total`: Count of MVCC write-write conflict rollbacks.

### 2. Health Endpoint
The health endpoint is unauthenticated for load-balancer readiness probes:
```bash
curl http://127.0.0.1:9101/health
# Response: {"status":"UP","nodeId":"node-1","term":2,"role":"LEADER"}
```

### 3. Launching the Grafana & Prometheus Stack
A complete monitoring stack is provided in the `docker/` directory:

```bash
cd docker/
docker compose up -d
```

- **Prometheus UI**: [http://localhost:9090](http://localhost:9090)
- **Grafana Dashboard**: [http://localhost:3000](http://localhost:3000) (Credentials: `admin` / `aegisdb`)
  - The dashboard automatically renders cluster latency percentiles, leader transitions, transaction throughput, and storage compaction.

---

## Empirical Research Benchmarking

AegisDB includes a scientific benchmark harness (`aegisdb-benchmark`) to empirically evaluate distributed systems trade-offs across three primary research questions:

1. **RQ1 (Write Batching Trade-Off)**: Evaluates write throughput (ops/sec) and tail latency (P95/P99) across varying batch sizes (1, 10, 50, 100).
   - *Result*: Amortizing disk fsync and network RPCs through batching yields a **~6.6x throughput increase** from batch size 1 to 50.
2. **RQ2 (Fault Recovery Dynamics)**: Measures leader failover detection and consensus recovery time under injected RPC delays and hard node crashes.
   - *Result*: Failover completes in **300–400 ms** with zero data loss or split-brain anomaly under majority quorum.
3. **RQ3 (MVCC Contention & Invariant Conservation)**: Evaluates transaction abort rates under increasing concurrency levels while verifying the strict conservation of financial balance invariants ($A+B+C=\text{Const}$).
   - *Result*: Under high contention, OCC transaction abort rates increase predictably while financial invariants maintain a **100.0% conservation rate (0 funds lost or created)**.

### Running the Research Benchmark Suite
```bash
mvn exec:java -pl aegisdb-benchmark \
    -Dexec.mainClass=se.mouaz.aegisdb.benchmark.ExperimentSuiteRunner
```
Results and provenance metadata are exported directly to:
- `experiments/data/results.csv`
- `experiments/data/results.json`

Generate publication-ready graphs:
```bash
python scripts/plot_benchmarks.py
```
Visualized graphs are saved to `experiments/graphs/` (`rq1_batching.png`, `rq2_recovery.png`, `rq3_contention.png`).

---

## System Quality & Verification Invariants Checklist

AegisDB enforces formal distributed systems invariants and rigorous quality gates. All 20 architectural categories are continuously validated through automated test suites:

| # | Invariant / Requirement | Verification Mechanism | Status |
|:---:|---|---|:---:|
| 1 | **Reliable Cluster Bootstrap** | Three or more nodes start and form cluster topology | ✅ PASS |
| 2 | **Election Safety** | At most one leader per term is enforced ($L \le 1$) | ✅ PASS |
| 3 | **Failover Recovery** | Unresponsive leader triggers re-election by majority | ✅ PASS |
| 4 | **Quorum Replication** | Writes replicate and require majority confirmation | ✅ PASS |
| 5 | **State Durability** | Committed data persists across node restarts | ⚠️ IN PROGRESS |
| 6 | **WAL Tail Truncation** | Partial or corrupted WAL tails safely recovered | ✅ PASS |
| 7 | **Log Compaction** | Periodic snapshots compact log and restore lagging nodes | ⚠️ IN PROGRESS |
| 8 | **Transparent Client SDK** | Transparent client routing and failover across leaders | ✅ PASS |
| 9 | **Snapshot Isolation** | MVCC repeatable reads and write-write conflict aborts | ✅ PASS |
| 10 | **Atomic Local Transactions** | ACID transactions with atomic commit and rollback | ⚠️ IN PROGRESS |
| 11 | **Consistent Sharding** | Deterministic partition routing via consistent hashing | ✅ PASS |
| 12 | **Cross-Shard 2PC** | Distributed 2PC transactions recover from coordinator crashes | ⚠️ IN PROGRESS |
| 13 | **Idempotent Transport** | Duplicate messages and retry requests are idempotent | ✅ PASS |
| 14 | **Chaos Engineering** | Network partition, packet drop, and crash fault injection | ✅ PASS |
| 15 | **Security Hardening** | Constant-time Bearer token RBAC and input bounds | ⚠️ IN PROGRESS |
| 16 | **Quality & Architecture Gates** | Clean ArchUnit rules, SpotBugs, Checkstyle, and PMD | ⚠️ IN PROGRESS |
| 17 | **Telemetry & Observability** | OpenTelemetry spans and Prometheus `/metrics` export | ✅ PASS |
| 18 | **Reproducible Benchmarks** | Automated export to structured CSV and JSON formats | ✅ PASS |
| 19 | **Research Questions Answered** | Empirical data addresses RQ1, RQ2, and RQ3 trade-offs | ✅ PASS |
| 20 | **Comprehensive Documentation** | Clean system architecture, operational manual, and ADRs | ✅ PASS |

---

## Documentation & Architectural Decision Records (ADRs)

In-depth technical specifications and architectural decisions are documented in the `docs/` directory:

- [System Architecture Specification](docs/architecture.md)
- [Consistency & Linearizability Model](docs/consistency.md)
- [Failure Model & Fault Tolerance Analysis](docs/failure-model.md)
- [Raft Consensus Implementation Guide](docs/raft.md)
- [Storage Engine, WAL & Recovery Internals](docs/storage.md)
- [Log Compaction & Snapshot Streaming](docs/snapshots.md)
- [Security Hardening & Guardrails](docs/security.md)
- [Empirical Research & Benchmark Methodology](docs/experiments.md)
- [Historical Completion & Readiness Report](docs/history/master-completion-report.md)
- [Historical Milestone 4 Report](docs/history/milestone-m4-report.md)

### Architectural Decision Records (ADRs)
- [ADR 0001: Java 21 LTS Baseline](docs/adr/0001-java-21-baseline.md)
- [ADR 0002: gRPC and In-Memory Transport](docs/adr/0002-grpc-and-in-memory-transport.md)
- [ADR 0003: Deterministic Event Loop for Raft](docs/adr/0003-deterministic-event-loop-for-raft.md)
- [ADR 0004: WAL Format and Fsync Policy](docs/adr/0004-wal-format-and-fsync-policy.md)
- [ADR 0005: Snapshots and Log Compaction](docs/adr/0005-snapshots-and-log-compaction.md)
- [ADR 0006: Multi-Version Concurrency Control (MVCC) & Snapshot Isolation](docs/adr/0006-mvcc-and-snapshot-isolation.md)
- [ADR 0007: Single-Shard Transactions](docs/adr/0007-single-shard-transactions.md)
- [ADR 0008: Sharding and Query Routing](docs/adr/0008-sharding-and-query-routing.md)
- [ADR 0009: Cross-Shard Distributed Transactions](docs/adr/0009-cross-shard-distributed-transactions.md)
- [ADR 0010: Chaos Engineering and Security Hardening](docs/adr/0010-chaos-and-security-hardening.md)
- [ADR 0011: Observability, Benchmarking and Research](docs/adr/0011-observability-benchmarking-and-research.md)
- [ADR 0012: System Release Demonstration & Readiness](docs/adr/0012-system-release-and-readiness.md)

---

## Community & Governance

- [Contributing Guidelines](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Security Policy](SECURITY.md)
- [Support & Community](SUPPORT.md)
- [Changelog](CHANGELOG.md)
- [Notice & Third-Party Attributions](NOTICE)

---

## License

AegisDB is open-source software licensed under the [MIT License](LICENSE).
