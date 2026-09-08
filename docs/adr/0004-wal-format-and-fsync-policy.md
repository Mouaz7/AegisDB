# ADR 0004: WAL Record Framing, CRC32 Checksums, and Fsync Policy

## Status
Accepted

## Context
AegisDB is a fault-tolerant distributed transactional database engine. Raft consensus requires committed log entries and node state (`currentTerm`, `votedFor`) to survive crashes, sudden power failures, and restarts (US007, US008). 

Without deterministic file framing, checksum validation, and explicit fsync policies:
1. Process termination mid-write causes torn records that corrupt the log.
2. Silent bit rot or disk failure goes undetected, risking consensus state divergence.
3. Unbounded read allocations risk Out-of-Memory crashes when reading malformed headers.

## Decision
1. **Module Separation**: We establish `aegisdb_storage` with zero dependencies on gRPC or Spring.
2. **Framing Standard**: Every WAL record includes:
   - 4-byte Magic Number (`0xAE615DA1`)
   - 2-byte Version (`1`)
   - 1-byte Record Type
   - 4-byte Length prefix (bounded between 36B and 16MB)
   - 4-byte CRC32 Checksum over body fields
   - 8-byte Sequence Number (LogIndex)
   - 8-byte Timestamp
   - 8-byte Raft Term
   - Key & Value variable payloads
3. **Atomic Consensus Metadata**: `currentTerm` and `votedFor` are saved using write-to-temp, `force(true)`, and `ATOMIC_MOVE` atomic rename.
4. **Fsync Policy**: Default to `FsyncPolicy.ALWAYS` for consensus logs (`FileChannel.force(true)` on each append).
5. **Partial-Write Recovery**: Torn writes at EOF are safely detected and truncated to the last intact offset during node recovery.

## Consequences
### Positive
- Strict durability guarantees: committed data survives process crash and restart.
- Bit flips and corruption are instantly detected with `CorruptedWalException`.
- Torn tail bytes from power failure are automatically truncated without manual intervention.
- Memory lookups remain $O(1)$ via in-memory `StorageIndex`.

### Negative
- Synchronous `force(true)` per write introduces disk I/O latency (mitigated in future sprints by write batching).
