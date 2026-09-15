# ADR 0012: System Release Demonstration, Readiness Verification Gates, and Release Packaging

## Status
Accepted

## Context
AegisDB has traversed eleven iterative engineering phases (Phase 0 through Phase 11), successfully realizing Raft consensus, persistent WAL storage, snapshots, MVCC, single-shard ACID transactions, horizontal multi-Raft sharding, cross-shard Two-Phase Commit (2PC), chaos fault injection, security hardening, and OpenTelemetry observability with empirical research benchmarks.

To fulfill the overarching public release readiness and quality criteria, a formalized system release process is required to:
1. **Unify all system subsystems into the System Release Demonstration Scenario**:
   - Execute the complete 16-step operational lifecycle: cluster bootstrap, leader election, log replication, concurrent workloads, leader kill failover, quorum recovery, old leader restart and catch-up, concurrent MVCC transactions with Snapshot Isolation, cross-shard 2PC atomic commits, minority network partitions, partition healing, secure management REST endpoints with RBAC Bearer token authentication, OpenTelemetry tracing/metrics with Prometheus exposition, and reproducible research benchmark exports.
2. **Formalize the 28-Point System Release Readiness Checklist**:
   - Provide an auditable, automated verification gate confirming that every single requirement is fulfilled, tested, and validated by executable test evidence.
3. **Establish a Production Distribution Pipeline**:
   - Package all compiled binaries, configuration files, CLI launchers, and third-party dependencies into standalone distributable bundles (`aegisdb-0.1.0-alpha.1-bin.tar.gz` and `.zip`) alongside the containerized Docker Compose environment.
4. **Enforce Dual Verification**:
   - Provide an automated regression test suite (`SystemReleaseIntegrationTest.java`) integrated into Maven and GitHub Actions CI, alongside operational smoke test scripts (`scripts/run-release-smoke-test.sh` and `scripts/verify-release-readiness.sh`).

## Decision
1. **Automated System Release Regression Suite (`SystemReleaseIntegrationTest.java`)**:
   - Standard JUnit 5 test class validating the complete 16-step scenario non-interactively within `mvn test`.
2. **Production Release Packager (`scripts/package-release.sh`)**:
   - Generates release artifacts in `target/release/`:
     - `bin/`: Standalone bash launcher scripts (`aegisdb-server`).
     - `lib/`: All compiled module JARs and third-party runtime dependencies.
     - `config/`: Production YAML templates (`aegisdb-cluster.example.yaml`).
     - `docs/`: Comprehensive architecture documentation, failure models, and ADRs.
     - Cryptographic SHA-256 checksums (`aegisdb-0.1.0-alpha.1-bin.tar.gz.sha256`).
3. **Release Readiness Verification Script (`scripts/verify-release-readiness.sh`)**:
   - Authoritative automated mapping of all 28 readiness items to concrete source classes, test suites, and verification outcomes.

## Consequences
### Positive
- **Defensible System Release Deliverable**: Proves that AegisDB is not merely a collection of isolated modules, but a unified, resilient distributed transactional database engine.
- **Reproducible Verification**: Anyone cloning the repository can reproduce the entire verification in a single command (`./scripts/verify-release-readiness.sh`).
- **Release Ready**: Provides turnkey binary packaging with external runtime dependencies for immediate evaluation.

### Negative / Trade-offs
- The 16-step end-to-end scenario runs multiple in-memory nodes, consensus elections, and 2PC transactions sequentially, requiring ~20-30 seconds of execution time during a complete CI run.
