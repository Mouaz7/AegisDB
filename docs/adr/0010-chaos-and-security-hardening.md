# ADR 0010: Chaos Engineering, Fault Injection, and Security Hardening

## Status
Accepted

## Context
Following the completion of Cross-Shard Distributed Transactions and Milestone M4 in Phase 9 (ADR 0009), AegisDB requires resilient fault tolerance under arbitrary network/node failures, as well as operational boundary defense and security controls per Master Project Plan §3, §4, §5, §10, §11, §12, §14, §15, §17, §18 & §20 (Phase 10; US016, US017).

Key requirements:
1. **Reproducible Fault Injection (US016)**: Researcher ability to deterministically inject message drops, artificial latency delays, packet duplications, and network partitions with seeded pseudo-randomness.
2. **Node Lifecycle & Failures**: Controlled kills of cluster leaders and followers, verifying automatic re-election, catch-up, and absence of data loss.
3. **Safety Invariant Verification**: Continuous verification during adverse conditions of:
   - At most one leader per term.
   - Non-decreasing monotonic terms.
   - Committed log prefix equality.
   - Financial conservation invariant ($A + B + C = 3000$) across multi-shard 2PC transfers.
4. **Secure Management Plane (US017)**: Operational REST API (`/health`, `/node`, `/cluster`, `/raft`, `/shards`, `/transactions`, `/metrics`, `/admin/snapshot`, `/admin/stepdown`) secured via Bearer token authentication and Role-Based Access Control (RBAC).
5. **Resource Bounding & Input Guards**: Bounded key sizes (max 1KB), value payloads (max 16MB), batch transactions, rate limiting to prevent DoS, and path traversal defense against directory escapes.
6. **Strict Clean Architecture**: Core consensus and storage must have zero dependencies on management or chaos modules (Master Plan §11).

## Decision
1. **Dedicated Modules**:
   - `aegisdb_chaos`: Houses `FaultyTransport`, `FaultRule`, `ChaosOrchestrator`, and `ChaosInvariantMonitor`.
   - `aegisdb_management`: Houses `ManagementHttpServer`, `ManagementSecurityManager`, `RateLimiter`, and `SecurityGuardrails`.
2. **Composable Transport Decorator**:
   - `FaultyTransport` implements `RaftTransport` and wraps any underlying transport (`InMemoryTransport`, `GrpcRaftTransport`).
   - Uses a seeded `Random` generator for deterministic replayability.
3. **Continuous Invariant Monitor**:
   - `ChaosInvariantMonitor` runs alongside fault scenarios to assert consensus safety and financial invariants in real time.
4. **Lightweight Secure Management API**:
   - Uses standard JDK `HttpServer` with virtual threads, eliminating external heavyweight framework bloat in Java 21 LTS.
   - Constant-time secret validation (`MessageDigest.isEqual`) prevents timing side-channels.
   - Enforces RBAC (`ROLE_MONITOR` vs `ROLE_ADMIN`) with deny-by-default on unauthorized endpoints.
5. **Security Guardrails**:
   - `SecurityGuardrails` validates inputs and sanitizes filesystem subpaths to block directory traversal (`..` escapes).

## Consequences
### Positive
- **Deterministic Chaos Research**: Fault scenarios are 100% reproducible via seed configuration.
- **Continuous Invariant Guarantees**: Consensus and transaction safety are proven under combined node kills, partitions, drops, and delays.
- **Defense in Depth**: Zero unauthenticated management operations, bounded inputs, rate limiting against DoS, and path traversal protection.
- **Architectural Purity**: Core consensus and storage remain completely decoupled from chaos and management adapters per ArchUnit rules.

### Negative / Trade-offs
- In-flight requests dropped by chaos trigger client retries or timeouts as intended by design.
