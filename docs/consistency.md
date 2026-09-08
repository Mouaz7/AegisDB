# AegisDB Consistency & Durability Model

---

## 1. Consistency Model

AegisDB provides well-defined consistency guarantees for both individual key-value operations and distributed transactions:

### Key-Value Operations (Raft Group)
- **Linearizability (Strong Consistency):**
  All confirmed writes and reads within an individual Raft group are linearizable. Once a write is acknowledged with `SUCCESS`, all subsequent reads are guaranteed to observe either that value or a newer committed value.

### Transaction Level (MVCC)
- **Snapshot Isolation (Standard):**
  Transactions read from a consistent snapshot established at transaction start.
  - Readers never block writers, and writers never block readers.
  - Prevents Dirty Reads, Non-Repeatable Reads, and Lost Updates.
- **Serializable Validation (Advanced Milestone):**
  Validation of ReadSet and WriteSet at commit time to detect and abort conflicting transactions (e.g., Write Skew).

---

## 2. Durability Guarantee

> **No `SUCCESS` response is returned to the client before the defined durability guarantees are satisfied.**

When a client receives a `SUCCESS` confirmation:
1. The log entry has been committed by quorum (strict majority of Raft nodes).
2. Data has been physically persisted to disk via the Write-Ahead Log (WAL) according to the active `fsync` policy.
3. The committed state survives crashes and restarts of up to $\lfloor(N - 1) / 2\rfloor$ nodes without data loss.
