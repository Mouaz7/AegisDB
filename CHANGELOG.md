# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0-alpha.1] - 2026-09-15

### Added
- **Raft Consensus Engine (`aegisdb-raft`)**: Randomized leader election, term validation, log replication with majority quorum commit, and log compaction with chunked snapshot transfers.
- **Durable Storage Engine (`aegisdb-storage`)**: Binary Write-Ahead Log (WAL) framing with magic bytes (`0xAE615D80`), CRC32 checksums, configurable `fsync` policies, and automatic recovery with tail-corruption repair.
- **MVCC Concurrency (`aegisdb-mvcc`)**: Lock-free multi-version read snapshots under Snapshot Isolation (SI), first-committer-wins write conflict detection, and background version garbage collection.
- **Single & Cross-Shard Transactions (`aegisdb-transaction`)**: ACID transactions within shards, and distributed Two-Phase Commit (2PC) coordination with binary coordinator logging and crash recovery across all failure modes.
- **Horizontal Sharding (`aegisdb-sharding`)**: Consistent hashing ring with virtual nodes and deterministic query routing.
- **Observability & Telemetry (`aegisdb-observability`)**: OpenTelemetry lock-free counters and latency histograms with Prometheus exposition format `/metrics`.
- **Management API & Security (`aegisdb-management`)**: HTTP REST diagnostic server with loopback binding, constant-time Bearer token RBAC authentication (`ROLE_MONITOR`, `ROLE_ADMIN`), rate limiting, and input bounding guardrails.
- **Chaos Engineering (`aegisdb-chaos`)**: Deterministic fault injection for network partitions, delays, drops, packet duplication, and safety invariant monitors.
- **Client SDK (`aegisdb-client`)**: High-level Java client with transparent cluster topology discovery and retry mechanisms.
- **Release Packaging**: Standalone distribution packaging (`bin/`, `lib/`, `config/`, `docs/`) with all runtime dependencies, SHA-256 checksums, and CycloneDX SBOM generation.
- **Engineering Quality Gates**: Automated 28-point readiness checklist (`verify-release-readiness.sh`), ArchUnit boundary gates, Maven Enforcer, and GitHub Actions CI pipelines.
