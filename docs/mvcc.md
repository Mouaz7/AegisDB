# AegisDB Multi-Version Concurrency Control (MVCC) Engine

## 1. Overview
The `aegisdb-mvcc` module implements a high-throughput, lock-free Multi-Version Concurrency Control (MVCC) storage engine providing **Snapshot Isolation (SI)** as defined in Master Project Plan §9 & §17 (Phase 6, US011).

In AegisDB:
- **Readers never block writers.**
- **Writers never block readers.**
- **Reads are completely lock-free.**
- **Point-in-time snapshots guarantee repeatable, consistent reads.**
- **Garbage collection operates safely under watermark thresholds.**

---

## 2. Core Architecture & Classes

```
                          +------------------------+
                          |   TimestampProvider    |
                          | (Monotonic Logical Ts) |
                          +-----------+------------+
                                      |
                                      v
+------------------+       +----------+------------+       +-------------------+
|     Snapshot     |<------|       MvccStore       |------>|   MvccGarbage     |
| (Point-in-time)  |       | (Concurrent Map & Tx) |       |    Collector      |
+------------------+       +----------+------------+       +-------------------+
        |                             |
        |                             v
        |                  +----------+------------+
        |                  |      VersionChain     |
        |                  |  (Atomic Head / CAS)  |
        |                  +----------+------------+
        |                             |
        v                             v
+------------------+       +----------+------------+
|  VisibilityRule  |<------|     VersionedValue    |
| (Pure Engine)    |       | (Immutable Node / SI) |
+------------------+       +-----------------------+
```

### 2.1 Core Components

| Component | Responsibility | Master Plan Reference |
| :--- | :--- | :--- |
| `VersionedValue` | Immutable node in a version chain storing `(createTxId, commitTimestamp, value, isTombstone, next)`. | §9 MVCC classes |
| `VersionChain` | Singly-linked list per key with atomic CAS head updates. Supports non-blocking traversal and watermark pruning. | §9 MVCC classes |
| `TimestampProvider` | Generates monotonically increasing logical timestamps for transactions and snapshots. | §9 MVCC classes |
| `Snapshot` | Immutable point-in-time read view tracking `readTimestamp`, reader transaction ID, and in-flight transaction set. | §9 MVCC classes |
| `VisibilityRule` | Pure, stateless evaluator implementing the 6 canonical Snapshot Isolation visibility rules. | §9 Visibility rules |
| `MvccStore` | Thread-safe multi-version key-value store coordinating transactions, write-lock checks, point-in-time scans, and Raft snapshot serialization. | §9 MVCC classes |
| `MvccGarbageCollector` | Background/on-demand watermark pruner reclaiming obsolete historical version nodes and tombstone keys. | §9 MVCC classes |

---

## 3. Visibility Rules & Snapshot Isolation

When a reading transaction accesses a key using a `Snapshot`, the version chain is traversed from newest to oldest. For each candidate `VersionedValue`, `VisibilityRule.isVisible(version, snapshot)` evaluates:

```
[Candidate VersionedValue]
           |
           +---> Is aborted? --------------------------> NO (Invisible)
           |
           +---> Created by own active transaction? ---> YES (Read-Your-Own-Writes)
           |
           +---> Is uncommitted? ----------------------> NO (Dirty Read Prevention)
           |
           +---> Was tx active at snapshot start? -----> NO (Invisible)
           |
           +---> Commit TS > Snapshot Read TS? --------> NO (Future Version Invisible)
           |
           +---> Commit TS <= Snapshot Read TS? -------> YES (Committed Visible)
```

### Anomaly Protections

| Anomaly | Status | Mechanism |
| :--- | :--- | :--- |
| **Dirty Read** | **Prevented** | Uncommitted versions created by other transactions are strictly invisible. |
| **Lost Update** | **Prevented** | First-Committer-Wins validation rejects writes if the key was committed after reader's start timestamp. |
| **Non-Repeatable Read** | **Prevented** | Snapshot reads always return the version valid at the snapshot's timestamp regardless of subsequent commits. |
| **Write-Write Conflict** | **Prevented** | Concurrent modifications to the same key are caught via active write locks and committed timestamp verification. |
| **Write Skew** | **Documented** | Snapshot Isolation allows write skew on disjoint keys; Phase 7 adds Serializable conflict validation. |

---

## 4. Garbage Collection & Watermarking

Garbage collection runs concurrently or on-demand without blocking active reads or writes.

### Safety Invariant
> **"Garbage collection must not delete a version still visible to an active snapshot."** (Master Plan §9)

### Algorithm
1. **Watermark Calculation**:
   $$\text{Watermark} = \min_{s \in \text{ActiveSnapshots}} s.\text{readTimestamp}$$
   If no snapshots are active, $\text{Watermark} = \text{CurrentLogicalTimestamp}$.
2. **Version Pruning per Key**:
   - All versions with $\text{commitTimestamp} > \text{Watermark}$ are **retained** (needed for newer/future snapshots).
   - The **single newest committed version** with $\text{commitTimestamp} \le \text{Watermark}$ is **retained** as the baseline for snapshots reading at the watermark.
   - All older committed versions with $\text{commitTimestamp} < \text{Watermark}$ are **pruned**.
3. **Tombstone Key Reclamation**:
   - If a key's chain contains only a committed tombstone (deleted record) and no older versions remain, the key is removed from the store's index.

---

## 5. Raft State Machine Integration

To support Raft log compaction and snapshot transfer (Phase 5, US009/US010):
- `MvccStore.serializeSnapshot()` exports all committed, visible, non-tombstone entries into a compact binary stream framed with CRC32 checksums:
  - Magic: `0x4D564343` ("MVCC")
  - Version: `1`
  - Snapshot Timestamp: 8 bytes
  - Record Count: 4 bytes
  - CRC32 Checksum: 8 bytes
  - Payload entries: `[keyLength (2B) | keyBytes | valueLength (4B) | valueBytes]`
- `MvccStore.restoreSnapshot(byte[])` atomically clears existing state, verifies the CRC32 checksum, and populates fresh version chains.

---

## 6. Verification and Testing

All Phase 6 acceptance criteria and Master Plan §9 requirements are verified by automated tests:

- `VisibilityRuleTest`: 8 tests verifying all 6 visibility edge cases and null safety.
- `VersionChainTest`: 6 tests verifying chain traversal, commits, aborts, and pruning.
- `MvccStoreTest`: 8 tests verifying transactional isolation, auto-commit, scan, and Raft snapshot restoration.
- `MvccGarbageCollectorTest`: 3 tests verifying watermark safety and tombstone cleanup.
- `MvccConcurrencyTest`: Verifies non-blocking readers under high-concurrency writes and the bank invariant ($A + B + C = 3000$).
- `MvccAnomalyTest`: 5 dedicated tests for Dirty Read, Lost Update, Non-Repeatable Read, Write-Write Conflict, and Write Skew.
- `MvccArchitectureTest`: ArchUnit tests proving zero dependencies on Spring, gRPC, management, chaos, or benchmark modules.
