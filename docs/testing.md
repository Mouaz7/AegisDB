# AegisDB Test Strategy & Verification Framework

## 1. Overview
The AegisDB test strategy enforces correctness, durability, and fault tolerance across multiple testing layers (Master Project Plan §14 & §25).

A core design rule across all test suites:
> **No Sleep-Driven Tests:** Core consensus and concurrency tests must prefer deterministic schedulers (`DeterministicScheduler`), test clocks (`TestClock`), and condition-based polling (`Awaitility`). Arbitrary `Thread.sleep` calls are strictly avoided to eliminate test flakiness.

---

## 2. Testing Layers

| Layer | Purpose | Key Test Suites |
| :--- | :--- | :--- |
| **Unit** | Pure algorithm behavior without I/O or network dependencies. | `RaftLogTest`, `VisibilityRuleTest`, `FileRaftMetadataStorageTest` |
| **Property** | Broad state-space invariant checks and serialization round-trips. | `LogPrefixPropertyTest`, `SnapshotSerializationTest`, `WalChecksumTest` |
| **Integration** | Real module composition and multi-node interactions. | `ThreeNodeElectionIntegrationTest`, `WalRecoveryIntegrationTest`, `SnapshotRecoveryIntegrationTest` |
| **Concurrency** | Race condition detection and contention verification under load. | `MvccConcurrencyTest`, `MvccAnomalyTest`, `ReplicatedKeyValueStoreTest` |
| **Recovery** | Crash, corruption, and restart verification. | `WalTornTailRecoveryTest`, `SnapshotCompactionTest`, `ProcessRestartTest` |
| **Chaos** | Safety under simulated node kills, network delays, packet drops, and partitions. | `LeaderKillIntegrationTest`, `PartitionCommitBlockedTest`, `FollowerCatchupTest` |
| **Architecture** | Executable constraints via ArchUnit ensuring module purity. | `RaftArchitectureTest`, `StorageArchitectureTest`, `MvccArchitectureTest` |

---

## 3. Bank Transfer Invariant Test
To prove that Snapshot Isolation and concurrency controls guarantee serializable consistency without money loss:
- **Initial Balances**: Account A = 1000, Account B = 1000, Account C = 1000 (Total = 3000).
- **Execution**: Hundreds of concurrent transactions transfer funds between randomly selected accounts.
- **Required Invariant**:
  $$\text{Balance}(A) + \text{Balance}(B) + \text{Balance}(C) = 3000$$
  This invariant is verified in `MvccConcurrencyTest` and `ReplicatedKeyValueStoreTest`.

---

## 4. Quality Gates & CI Pipeline
Merges to `main` require passing the strict verification gate:
```bash
mvn clean verify
```
Quality gates fail if:
1. Compilation fails.
2. Any unit, integration, or architecture test fails.
3. Architecture rules in ArchUnit are violated.
4. Static analysis exceeds configured severity.
5. Code coverage falls below module thresholds.
