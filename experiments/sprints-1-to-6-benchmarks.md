# AegisDB: Comprehensive Stress & Performance Benchmark Evaluation (Sprints 1 to 6)

## Overview

This report documents the performance characteristics, concurrency stress test results, and invariant verifications of the **AegisDB** distributed database engine across Sprints 1 through 6. The benchmarks evaluate the engine under high multi-threaded contention, sustained churn, synchronous/asynchronous disk I/O, and multi-node consensus log replication.

---

## Benchmark Environment

- **Architecture:** x86_64, Linux kernel (WSL2) on Windows host
- **Runtime:** OpenJDK 25 (Java 25 LTS)
- **Concurrency Framework:** Virtual and platform threads with lock-free data structures (`AtomicReference`, `ConcurrentSkipListMap`, `ConcurrentHashMap`)
- **Persistence Target:** Local SSD with CRC32 frame checksums and zero-copy NIO channels

---

## Summary Results

| Benchmark Suite | Total Ops | Throughput (ops/sec) | Latency (P50) | Latency (P95) | Latency (P99) | Invariant Status |
|---|---|---|---|---|---|:---:|
| **MVCC Read-Heavy (80/20, 16 Threads)** | 20,000 | **84,181.7 ops/s** | 0.00 ms | 0.98 ms | 2.91 ms | ✅ 100% Invariant Pass |
| **MVCC High Contention (Bank Transfers, 5 Hot Accounts)** | 5,000 | **19,167.0 ops/s** | 0.15 ms | 2.80 ms | 4.59 ms | ✅ Balance Conservation $\Delta = 0$ |
| **MVCC Heavy Churn (Active Snapshot Watermark)** | 50,000 | **1,780.3 ops/s** | 0.01 ms | 8.24 ms | 16.06 ms | ✅ Snapshot View Preserved |
| **WAL Append Throughput (`PERIODIC` fsync)** | 10,000 | **68,591.8 ops/s** | 0.00 ms | 0.02 ms | 0.05 ms | ✅ 100% CRC32 Verified |
| **WAL Cold Recovery & Checksum Validation** | 10,000 | **105,781.0 ops/s** | 94.53 ms | 94.53 ms | 94.53 ms | ✅ Zero Corruption / Full Recovery |
| **Raft 3-Node In-Memory Consensus Replication** | 1,000 | **590.3 ops/s** | 0.50 ms | 14.59 ms | 19.36 ms | ✅ Quorum Monotonicity & Consensus |

---

## Detailed Benchmark Analysis

### 1. MVCC Read-Heavy Workload (Sprint 6, Master Plan §9 & §17)
- **Configuration:** 16 concurrent threads executing 20,000 total operations with an 80% read / 20% write distribution across 100 unique keys.
- **Results:**
  - **Throughput:** 84,181.7 ops/sec
  - **Latency:** P50: < 0.01 ms, P99: 2.91 ms
  - **Takeaway:** Readers achieve lock-free non-blocking traversal down the version chains. Writers never stall concurrent readers, delivering massive read scalability.

### 2. MVCC Bank Transfer Contention & First-Committer-Wins (Master Plan §9 & §17)
- **Configuration:** 16 concurrent threads transferring random amounts among only 5 hot accounts ($N=5$). Initial balance: 10,000 per account (Total = 50,000).
- **Invariants Checked:**
  - Strict First-Committer-Wins conflict detection.
  - Total balance conservation invariant ($Total = 50,000$, $\Delta = 0$).
- **Results:**
  - 5,000 operations evaluated under severe write conflicts (~79% write-conflict abort rate).
  - All aborted transactions rolled back cleanly without partial updates or memory leaks.
  - Final balances: `acc:0 = 9,930`, `acc:1 = 9,700`, `acc:2 = 9,660`, `acc:3 = 10,460`, `acc:4 = 10,200`.
  - **Final Total Balance:** **50,000** (Delta = 0).
  - **Takeaway:** Complete immunity against lost updates under extreme concurrent contention.

### 3. MVCC Heavy Churn & Watermarked Garbage Collection (Master Plan §9)
- **Configuration:** 50,000 rapid updates to a single key while a long-running snapshot remains active.
- **Invariants Checked:**
  - Garbage collection watermark calculation (`minActiveSnapshotTimestamp()`).
  - Active snapshots never observe reclaimed versions or broken chains.
- **Results:**
  - Initial chain accumulated 50,000 version nodes.
  - Garbage collector safely reclaimed obsolete versions up to the snapshot watermark.
  - Long-running snapshot read the exact point-in-time value it was initialized with.

### 4. WAL Write Throughput & Crash Recovery (Sprint 4, Master Plan §8 & §17)
- **Configuration:** 10,000 structured write-ahead log records appended sequentially to disk, followed by an abrupt simulated crash and cold recovery.
- **Results:**
  - Append throughput: **68,591.8 ops/sec** (P99 latency of 50 microseconds).
  - Cold crash recovery speed: **105,781.0 records/sec**.
  - All 10,000 records validated with CRC32 checksums, verifying segment rollover and header parsing.

### 5. Raft 3-Node In-Memory Replication (Sprints 1, 2, 3, 5, Master Plan §4 & §5)
- **Configuration:** Fully networked 3-node cluster (`bench-node-1`, `bench-node-2`, `bench-node-3`) connected via `InMemoryTransport` with real Raft election and log replication event loops.
- **Results:**
  - 1,000 sequential client proposals committed across quorum.
  - Verified 100% identical committed log sequences across all 3 nodes.
  - Zero dropped proposals, zero split-brain conditions, zero unhandled exceptions.

---

## How to Reproduce

Run the automated benchmark runner script:
```bash
./scripts/run-stress-benchmarks.sh
```

Or execute via Maven test harness:
```bash
mvn test -pl aegisdb_integration -Dtest=StressBenchmarkTest
```
