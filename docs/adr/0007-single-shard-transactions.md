# ADR 0007: Single-Shard Transactions, Concurrency Control, and Durable Logging

## Status
Accepted

## Context
Following the implementation of MVCC and Snapshot Isolation in Phase 6 (ADR 0006), AegisDB requires multi-operation ACID transaction processing on local shards per Master Project Plan §5, §9, §11, §14, §17, §18 & §20 (Phase 7 / US012; Milestone M3 Gate).

Key requirements:
1. **Atomic Multi-Operation Commits:** Clients must be able to group arbitrary read, put, and delete operations into a single atomic transaction that commits all changes or aborts cleanly with zero side-effects.
2. **Standard State Machine:** Enforce the 4-state lifecycle `ACTIVE -> PREPARING -> PREPARED -> COMMITTED / ABORTED` to provide direct 1-phase commits while preparing hooks for Phase 9 Two-Phase Commit (2PC).
3. **Read/Write Set Buffering:** Isolate uncommitted mutations in a local `WriteSet` with Read-Your-Own-Writes and bounded capacity limits to prevent memory exhaustion attacks (Master Plan §12).
4. **Comprehensive Conflict Detection:** Enforce First-Committer-Wins under Snapshot Isolation (preventing Lost Updates and Write-Write conflicts) and provide Serializable Snapshot Isolation (SSI) anti-dependency checking to prevent Write Skew.
5. **Durable Transaction Logging:** Persist lifecycle state transitions and write sets to disk via CRC32-framed segments (`DurableTransactionLog`) with torn-write truncation and restart recovery.
6. **Idempotency & Deduplication:** Support safe retries and duplicate suppression for client transactions (`ClientId + RequestId`).
7. **Architectural Decoupling:** Keep `aegisdb_transaction` strictly decoupled from gRPC, Spring, and outer management/chaos layers per ArchUnit rules (Master Plan §11).

## Decision
1. **Dedicated Module `aegisdb_transaction`**:
   - Created a standalone Maven module depending only on `aegisdb_common` and `aegisdb_mvcc`.
   - ArchUnit tests enforce zero dependencies on Spring, gRPC, Protobuf, management, benchmark, or chaos modules.

2. **4-Stage Transaction Lifecycle**:
   - Transactions progress through:
     `ACTIVE` (buffering in `WriteSet`) $\to$ `PREPARING` (running `CommitValidator`) $\to$ `PREPARED` (locks held) $\to$ `COMMITTED` (applied to `MvccStore`).
   - Terminal states (`COMMITTED`, `ABORTED`) are immutable; operations on closed transactions throw `IllegalStateException`.

3. **ReadSet, WriteSet & Bounded Resources**:
   - `ReadSet` records keys and observed commit timestamps.
   - `WriteSet` provides thread-safe buffering with Read-Your-Own-Writes.
   - Configurable `maxWriteSetSize` (default 10,000 keys) and `transactionTtl` (default 30 seconds) enforce bounded resource allocation and prevent hanging locks.

4. **Dual Isolation Models**:
   - `SNAPSHOT_ISOLATION`: Default mode preventing Dirty Read, Lost Update, Non-Repeatable Read, and Write-Write conflicts via First-Committer-Wins.
   - `SERIALIZABLE`: Advanced mode evaluating read-set anti-dependencies via `ConflictDetector.validateSerializableConflicts()`, preventing Write Skew anomalies.

5. **Durable TransactionLog with CRC32 Framing**:
   - Uses magic header `0xAE615D70`, versioning, record framing, and CRC32 checksums over headers and payloads.
   - Partial writes at EOF (torn tails from power failure) are safely detected and truncated on restart.
   - Provides `InMemoryTransactionLog` for fast deterministic simulation testing.

6. **Client SDK Integration**:
   - Extended `AegisDbClient` with `beginTransaction()` and `runInTransaction()` supporting automatic exponential backoff retry.
   - Added `LocalTransactionalClient` for local and single-shard testing.

## Consequences
### Positive
- **Guaranteed Consistency**: Preserves financial invariants under extreme concurrency ($A+B+C=3000$ verified across thousands of concurrent operations).
- **Extensible for 2PC**: The `PREPARING`/`PREPARED` states seamlessly integrate into Phase 9 Distributed Transactions without breaking single-shard workflows.
- **Crash Durability**: Physical disk durability with checksum validation ensures committed transactions survive restarts.
- **Strict Modularity**: Zero framework coupling ensures core transaction logic remains pure, fast, and easily testable.

### Negative / Trade-offs
- In-memory buffering in `WriteSet` consumes memory for large transactions; mitigated by `TransactionSizeLimitException`.
- Contention on hot keys requires backoff retry in client layers.
