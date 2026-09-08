# AegisDB

> **A Fault-Tolerant Distributed Transactional Database Engine in Java**

AegisDB is a distributed transactional database built from the ground up in Java 25. The project implements Raft consensus for strong consistency, fault-tolerant Write-Ahead Logging (WAL) and snapshots for persistent crash recovery, MVCC for isolated transactions, and horizontal sharding with Two-Phase Commit (2PC).

---

## Module Structure

The file and module structure is uniformly aligned with the repository name **AegisDB**:

```text
AegisDB/
├── pom.xml                   # Parent POM (Java 25, gRPC, Protobuf, JUnit 5)
├── README.md                 # Project documentation & instructions
├── LICENSE                   # MIT License
├── docker/                   # Docker and container configurations
├── docs/                     # Architecture, guarantees & failure models
│   ├── architecture.md       # System architecture & layer decoupling
│   ├── failure_model.md      # Failure model (fail-stop, crash-recovery, network)
│   ├── consistency.md        # Consistency model (Linearizability, Snapshot Isolation)
│   ├── raft.md               # Raft consensus engine documentation
│   ├── storage.md            # Storage engine, WAL & recovery documentation
│   └── snapshots.md          # Snapshot framing, chunked RPC & log compaction
├── scripts/                  # Build, test, and live demonstration scripts
├── experiments/              # Performance benchmarks and experiments
│
├── aegisdb_common/           # Domain models (NodeId, Endpoint, NodeStatus, Configs)
├── aegisdb_protocol/         # Protobuf contracts and gRPC RPC definitions
├── aegisdb_transport/        # Transport abstraction (InMemoryTransport, GrpcRaftTransport)
├── aegisdb_raft/             # Raft Consensus Engine (Election, Heartbeats, State, Invariants, Snapshots)
├── aegisdb_storage/          # Disk persistence, WAL, CRC32 Checksums, Snapshots & Recovery
├── aegisdb_node/             # Node lifecycle (DatabaseNode, NodeBootstrap, NodeLifecycle)
├── aegisdb_client/           # Java Client SDK (AegisDbClient, transparent redirect/retry)
└── aegisdb_integration/      # Acceptance tests & verification demos for Sprints 1-5
```

---

## Sprint 1: Cluster Foundation & Networking

Sprint 1 establishes the foundation for cluster communication and node management:
- **US003:** As an operator, I want to start multiple nodes.
- **US004:** As a node, I want to communicate with other nodes.

### Completed Acceptance Criteria (Sprint 1)

| Criterion | Description | Status |
|---|---|:---:|
| **Three nodes start** | Three nodes start concurrently and reach `RUNNING` status. | ✅ PASS |
| **Nodes have unique identities** | Each node is assigned a unique `NodeId` and dedicated `Endpoint` (port). | ✅ PASS |
| **Node A can call Node B** | RPC calls (`RequestVote`, `AppendEntries`) are dispatched and handled via gRPC and InMemory transports. | ✅ PASS |
| **Timeout works** | Unavailable or unresponsive nodes trigger a controlled timeout. | ✅ PASS |
| **Errors propagate correctly** | Invocations to stopped or unreachable nodes propagate cleanly as `TransportException`. | ✅ PASS |
| **Nodes stop gracefully** | Nodes terminate cleanly, releasing network and execution resources to reach `STOPPED` status. | ✅ PASS |

---

## Sprint 2: Raft Consensus Engine - Leader Election

Sprint 2 implements the Raft consensus engine leader election (US005) with a single-threaded event loop, randomized election timers, and strict safety invariants:
- **US005:** As a cluster, we want to elect a leader via Raft leader election.

### Completed Acceptance Criteria (Sprint 2)

| Criterion | Description | Status |
|---|---|:---:|
| **Exactly one leader exists per term** | At most one leader is elected per term (Election Safety Invariant). Election requires absolute quorum (`N/2 + 1`). | ✅ PASS |
| **Followers reset timeout after valid heartbeat** | The leader sends periodic heartbeats (`AppendEntries`). Followers reset their election timer and remain in follower state. | ✅ PASS |
| **New leader is elected after failure** | When a leader crashes, remaining nodes detect election timeout and safely elect a new leader in an incremented term. | ✅ PASS |

### Architecture & Invariants (Sprint 2)
- **Single-Threaded Event Loop**: All state transitions execute sequentially via a dedicated queue (`Executors.newSingleThreadExecutor`) eliminating concurrency race conditions.
- **Pluggable Clock & Scheduler Abstraction**: `Clock` (`SystemClock`, `TestClock`) and `Scheduler` (`SystemScheduler`, `DeterministicScheduler`) enable fast deterministic simulation testing without fragile sleeps.
- **Strict Architectural Decoupling**: ArchUnit architecture tests verify that `aegisdb_raft` has zero dependencies on Spring, management, benchmarks, or chaos modules.

---

## Sprint 3: Raft Consensus Engine - Log Replication

Sprint 3 implements Raft log replication (US006) per Ongaro §5.3/§5.4 with majority acknowledgments, commit index monotonicity, and automated conflict resolution:
- **US006:** As a leader, I want to replicate writes to a majority before commit.

### Completed Acceptance Criteria (Sprint 3)

| Criterion | Description | Status |
|---|---|:---:|
| **Writes replicate** | The leader receives client proposals (`propose`), appends to local `RaftLog`, and replicates entries to followers. | ✅ PASS |
| **Majority required** | Writes are committed only once acknowledged by a strict majority ($N/2 + 1$) of nodes. Minority partitions are safely blocked from committing. | ✅ PASS |
| **CommitIndex advances correctly** | `commitIndex` advances monotonically per Ongaro §5.3/§5.4 (leaders commit entries from the current term). | ✅ PASS |
| **Follower catches up** | A disconnected follower that reconnects automatically receives all missing entries and synchronizes to the leader's `commitIndex`. | ✅ PASS |
| **Conflicting entries are repaired** | Diverging, uncommitted entries in a follower's log are truncated and overwritten with authoritative leader entries via `LogConflictResolver`. | ✅ PASS |

### Architecture & Safety Invariants (Sprint 3)
- **1-based Log Indexing**: `RaftLog` uses 1-based sequence numbering with a sentinel entry at index 0.
- **Single-Threaded Event Loop**: Client writes (`ClientWriteEvent`) and replication responses (`AppendEntriesResponseEvent`) are processed sequentially without locks.
- **Core Raft Invariants**: Continuously verified at runtime and across test suites:
  - *Committed entries are never overwritten*
  - *Committed entries appear in identical order across all nodes*

---

## Sprint 4: Persistence and Recovery

Sprint 4 implements local fault-tolerant persistence and crash recovery per Master Project Plan §8 & §17 (US007 & US008):
- **US007:** As a database, I want committed data to survive crashes.
- **US008:** As a Raft node, I want term/votedFor to be durable across restarts.

### Completed Acceptance Criteria (Sprint 4)

| Criterion | Description | Status |
|---|---|:---:|
| **WAL segments and checksums** | Segment files (`wal-00000000000000000001.seg`) with automatic rollover at size thresholds and CRC32 checksums over headers and payloads. | ✅ PASS |
| **Flush/fsync policy** | Configurable synchronization policy (`ALWAYS`, `PERIODIC`, `MANUAL`). `ALWAYS` guarantees physical disk durability via `force(true)` before acknowledging writes. | ✅ PASS |
| **Persistent term/votedFor** | `currentTerm` and `votedFor` are saved atomically via write-to-temp + `fsync` + `ATOMIC_MOVE` before replying to RPCs (Ongaro §5.2). | ✅ PASS |
| **Partial-write recovery** | Incomplete writes at EOF (torn tails from sudden power loss) are automatically detected on startup and safely truncated to the last intact record. | ✅ PASS |
| **Corruption detection** | Bit flips, invalid magic numbers, or mismatched CRC32 checksums in existing records are instantly flagged with `CorruptedWalException`. | ✅ PASS |
| **Restart tests** | 3-node cluster survives abrupt process termination. All nodes recover term, votes, and log sequence from disk and resume consensus cleanly. | ✅ PASS |

### Architecture & Security (Sprint 4)
- **DurableRaftLog**: Drop-in extension of `RaftLog` providing synchronous write-through to WAL and durable `fsync` before in-memory state updates.
- **Path Traversal Protection**: Data directory validation prevents unauthorized directory traversal attacks.
- **Bounded Resource Allocations**: Record allocations are strictly bounded to 16 MB max to prevent memory exhaustion attacks from crafted headers.
- **Zero-Dependency Architecture**: `aegisdb_storage` maintains zero dependencies on Spring and gRPC.

---

## Sprint 5: Snapshots and Replicated Key-Value Store (Milestone M2 Gate)

Sprint 5 implements log compaction via snapshots, chunked `InstallSnapshot` RPC, a replicated Key-Value State Machine, and client SDK (US009 & US010; Milestone M2 Gate):
- **US009:** As a database node, I want snapshots so the log does not grow unbounded.
- **US010:** As a client, I want replicated key-value operations.

### Completed Acceptance Criteria (Sprint 5)

| Criterion | Description | Status |
|---|---|:---:|
| **Snapshot metadata and checksum** | Point-in-time snapshots (`snapshot-%020d-%020d.snap`) with Magic `0xAE615DA2`, versioning, framing, and CRC32 checksums. Atomic `.tmp` + `ATOMIC_MOVE` with retention of 2 latest snapshots. | ✅ PASS |
| **Snapshot install** | Chunked `InstallSnapshot` RPC over gRPC & InMemoryTransport with 64 KB chunking, offset, done flag, and CRC32 validation (Raft §7). | ✅ PASS |
| **KeyValueStateMachine** | Thread-safe `ConcurrentSkipListMap`-backed state machine supporting PUT, GET, DELETE with deterministic binary serialization and atomic snapshot restore. | ✅ PASS |
| **Client PUT/GET/DELETE** | Dedicated Java SDK module `aegisdb_client` with `AegisDbClient` and `DefaultAegisDbClient`. Provides automatic leader discovery and transparent redirect/retry on `NotLeaderException`. | ✅ PASS |
| **Follower catch-up from snapshot** | Disconnected or lagging followers whose missing log entries have been compacted away are automatically brought up to date via `InstallSnapshot`. | ✅ PASS |
| **Milestone M2 Gate** | 3-node persistent replicated key-value cluster survives leader crash, undergoes safe re-election, and enables clients to continue reads and writes without data loss. | ✅ PASS |

---

## Build and Run

### Prerequisites
- **Java 25 LTS**
- **Maven 3.8+**

### Run All Unit, Architecture, and Integration Tests
```bash
mvn clean test
```
or via script:
```bash
./scripts/test-all.sh
```

### Run Live Demonstration for Sprint 1 (Networking & Nodes)
```bash
./scripts/run-sprint1-demo.sh
```

### Run Live Demonstration for Sprint 2 (Raft Leader Election)
```bash
./scripts/run-sprint2-demo.sh
```

### Run Live Demonstration for Sprint 3 (Raft Log Replication)
```bash
./scripts/run-sprint3-demo.sh
```

### Run Live Demonstration for Sprint 4 (Persistence & Crash Recovery)
```bash
./scripts/run-sprint4-demo.sh
```

### Run Live Demonstration for Sprint 5 (Snapshots & Replicated KV Store / Milestone M2 Gate)
```bash
./scripts/run-sprint5-demo.sh
```
