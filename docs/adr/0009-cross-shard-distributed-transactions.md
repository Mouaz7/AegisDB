# ADR 0009: Cross-Shard Distributed Transactions & Two-Phase Commit (2PC)

## Status
Accepted

## Context
Following the implementation of Sharding and Multi-Raft Routing in Sprint 8 (ADR 0008), AegisDB requires cross-shard atomic transactions spanning multiple discrete Raft consensus groups per Master Project Plan §4, §5, §10, §11, §12, §14, §17, §18 & §20 (Sprint 9 / US015; Milestone M4 Gate).

Key requirements:
1. **Atomic Multi-Shard Commit & Rollback:** All operations across participating shards must commit together or abort cleanly with zero partial updates.
2. **Durable Coordinator Journaling:** Coordinator state transitions must be durably logged to disk with CRC32 checksums before issuing Phase 2 RPCs.
3. **Crash Recovery Matrix:** The system must handle all 8 failure modes described in Master Plan §10 (coordinator crash before/after decision, participant crash, partition, torn writes).
4. **Strict Serializability & Prepare Locking:** Participating shards must hold exclusive prepare locks during in-doubt windows, and validate read sets against MVCC to prevent Lost Updates.
5. **Transparent Client Integration:** The client SDK must coordinate 2PC automatically without exposing coordinator internals.

## Decision
1. **Two-Phase Commit State Machine**:
   - Coordinator transitions: `INIT` $\to$ `PREPARING` $\to$ `COMMIT_DECIDED` / `ABORT_DECIDED` $\to$ `COMMITTED` / `ABORTED`.
   - Parallel Phase 1 `PREPARE` with watchdog timeout.
   - Decision logged durably before Phase 2 fan-out.

2. **Durable Coordinator Log (`DurableCoordinatorLog`)**:
   - Append-only binary log with magic header `0xAE6120C0` and CRC32 framing.
   - Forced disk sync on decision transitions (`COMMIT_DECIDED`, `ABORT_DECIDED`).
   - Torn-write tail truncation on restart.

3. **Key-Level Locking & OCC Validation (`LocalShardParticipant`)**:
   - Exclusive prepare locks on write-set keys prevent concurrent writes or dirty reads.
   - Read-set anti-dependency check verifies current committed values against values read at transaction start.

4. **Crash Recovery Engine (`DistributedTransactionRecovery`)**:
   - Replays coordinator journal on startup and resolves in-doubt transactions across the 8 failure modes.

## Consequences
### Positive
- **Milestone M4 Gate Passed:** High-concurrency bank transfer invariant ($A + B + C = 3000$) strictly preserved across 3 shards.
- **Crash Durability:** System survives coordinator and participant crashes with zero lost state.
- **Strict Clean Architecture:** Core transaction module remains free of Spring, gRPC, and Protobuf dependencies.

### Negative / Trade-offs
- In-doubt locks hold resources until Phase 2 completes or recovery executes; watchdog timeout prevents indefinite blocking.
