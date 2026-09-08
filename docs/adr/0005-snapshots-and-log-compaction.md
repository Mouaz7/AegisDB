# ADR 0005: Snapshots, Log Compaction, and Replicated Key-Value State Machine

## Status
Accepted

## Context
In a Raft-replicated database, the log cannot grow without bound in practical systems (Raft paper §7). As write operations accumulate, WAL segments consume disk space and restart recovery time grows proportionally to log length. Furthermore, lagging or newly joined followers must be able to synchronize state without replaying history from log index 1.

Key design requirements:
1. State machine state must be periodically checkpointed into a compact snapshot.
2. Log entries up to `lastIncludedIndex` must be safely discarded/compacted.
3. Lagging followers whose `nextIndex <= snapshotIndex` must receive state via chunked `InstallSnapshot` RPC.
4. Client operations (PUT/GET/DELETE) must transparently handle leader elections, node failures, and redirects without data loss.

## Decision
1. **Snapshot Framing & Checksums**:
   - Every snapshot file is named `snapshot-%020d-%020d.snap` (`lastIncludedIndex`-`lastIncludedTerm`).
   - Binary framing:
     - 4-byte Magic Number (`0xAE615DA2`)
     - 2-byte Format Version (`1`)
     - 8-byte `lastIncludedIndex`
     - 8-byte `lastIncludedTerm`
     - 4-byte CRC32 Checksum computed over payload
     - 4-byte Payload Length prefix (bounded to 64MB)
     - Binary state machine payload
   - Atomic persistence: write to `.tmp` file, `force(true)`, and replace via `Files.move(..., ATOMIC_MOVE, REPLACE_EXISTING)`.
   - Snapshot retention: keep the 2 most recent valid snapshots, prune older files.

2. **Log Compaction**:
   - `RaftLog.compactUpTo(lastIncludedIndex, lastIncludedTerm)` retains snapshot sentinel index and offsets existing memory entries.
   - `WalManager.purgeSegmentsPriorTo(lastIncludedIndex)` safely deletes WAL segments whose maximum index is strictly less than the snapshot point.

3. **Chunked Snapshot Installation**:
   - `InstallSnapshot` RPC streams snapshot data in 64 KB chunks over gRPC and in-memory transports.
   - Follower buffers incoming chunks, verifies CRC32 and framing upon receiving `done=true`, and replaces state atomically.

4. **KeyValueStateMachine & Client SDK**:
   - `KeyValueStateMachine` uses `ConcurrentSkipListMap<String, byte[]>` for deterministic ordering and snapshot serialization.
   - Dedicated client module `aegisdb_client` provides `AegisDbClient` with automatic leader tracking, transparent redirect on `NotLeaderException`, and exponential backoff retry.

## Consequences
### Positive
- Bounded disk footprint and deterministic fast crash recovery.
- Lagging or disconnected nodes catch up quickly via snapshot transfer instead of replaying thousands of historical WAL entries.
- Clients experience seamless high-availability across leader failover.
- Achieves Milestone M2: 3-node persistent replicated KV store surviving leader failures.

### Negative
- Snapshot serialization temporarily holds a memory read-lock during snapshot capture.
- Network bandwidth is consumed during snapshot installation to lagging followers.
