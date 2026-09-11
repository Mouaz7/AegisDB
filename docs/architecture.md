# AegisDB Architecture

> **A Fault-Tolerant Distributed Transactional Database Engine in Java**

---

## 1. System Overview

AegisDB is a distributed, fault-tolerant transactional database written from the ground up in Java (Java 25 LTS). The system is built on Raft consensus for strong consistency and replication, combined with Write-Ahead Logging (WAL) and snapshotting for persistent storage and crash recovery, as well as MVCC (Multi-Version Concurrency Control) and Two-Phase Commit (2PC) for distributed transactions.

```mermaid
flowchart TD
    Client[AegisDB Client] --> Router[Query Router]
    Router --> S1[Shard 1]
    Router --> S2[Shard 2]
    Router --> S3[Shard 3]

    subgraph "Shard 1 (Raft Group 1)"
        N1[Node 1 - Leader] <--> N2[Node 2 - Follower]
        N1 <--> N3[Node 3 - Follower]
    end

    N1 --> TxLayer[Transaction Layer]
    TxLayer --> MVCC[MVCC Engine]
    MVCC --> Storage[Storage Engine]
    Storage --> WAL[Write Ahead Log]
    Storage --> Snap[Snapshot Store]
    WAL --> Disk[(Disk)]
    Snap --> Disk
```

---

## 2. Core Architectural Rules

The following layer boundaries are mandatory in the system design:

1. **Transport vs. Consensus:**
   Raft is entirely transport-agnostic and does not know whether RPC occurs over gRPC, HTTP/2, or an in-memory network (`InMemoryTransport`). All communication flows through the abstract `RaftTransport` interface.
2. **Storage vs. Transport:**
   The storage engine (`StorageEngine` and `WalManager`) has zero coupling to gRPC or network layers.
3. **Core vs. Framework:**
   The data engine core (Raft, WAL, MVCC, Transactions) maintains zero dependencies on Spring. Spring Boot is used exclusively in `aegisdb_management` for external administration and health APIs.

---

## 3. Module Structure

```text
AegisDB/
├── pom.xml                   # Root Parent POM (Java 25, gRPC, Protobuf, JUnit 5)
├── README.md                 # Project documentation & instructions
├── LICENSE                   # MIT License
├── docker/                   # Docker and container configurations
├── docs/                     # Architecture, guarantees & failure models
├── scripts/                  # Build, test, and live demonstration scripts
├── experiments/              # Benchmarks & research experiments
│
├── aegisdb_common/           # Domain models (NodeId, Endpoint, NodeStatus, TransactionId, ClientId, RequestId)
├── aegisdb_protocol/         # Protobuf contracts & gRPC RPC definitions
├── aegisdb_transport/        # GrpcRaftTransport & InMemoryTransport
├── aegisdb_node/             # DatabaseNode, NodeBootstrap & NodeLifecycle
├── aegisdb_integration/      # 3-node cluster tests & verification demos
├── aegisdb_raft/             # Raft Consensus, Election, Replication & Snapshots
├── aegisdb_storage/          # StorageEngine, WAL, CRC32 & Snapshots
├── aegisdb_client/           # Java SDK Client & Leader Redirect
├── aegisdb_mvcc/             # Multi-Version Concurrency Control (MvccStore, Snapshots, VersionChains)
├── aegisdb_transaction/      # Single-shard transactions, isolation levels, validation, conflict detection & durable logging
├── aegisdb_sharding/         # HashPartitioner, ShardMap & Routing
├── aegisdb_management/       # Lightweight REST Management API, RBAC Bearer Token Auth & Security Guardrails
├── aegisdb_observability/    # OpenTelemetry & Prometheus metrics
├── aegisdb_chaos/            # Fault injection, network partition & continuous invariant testing
└── aegisdb_benchmark/        # Latency & throughput benchmarks
```

---

## 4. Transaction & Concurrency Architecture

The `aegisdb_transaction` module provides atomic, single-shard ACID transactions with Snapshot Isolation (SI) and Serializable Snapshot Isolation (SSI):

- **Decoupled Architecture:** Zero coupling to transport, network, gRPC, or Spring frameworks. Depends solely on `aegisdb_common` and `aegisdb_mvcc`.
- **4-State Lifecycle:** Explicit state machine enforcing `ACTIVE -> PREPARING -> PREPARED -> COMMITTED` and `ACTIVE/PREPARING/PREPARED -> ABORTED`.
- **Isolation Modes:**
  - *Snapshot Isolation (SI):* Readers observe a consistent snapshot; concurrent writers detect collisions via First-Committer-Wins.
  - *Serializable Snapshot Isolation (SSI):* Tracks read-sets to detect and reject anti-dependency anomalies (such as write skew).
- **Durability & Recovery:** `DurableTransactionLog` guarantees crash safety using binary framing with magic headers (`0xAE615D70`), CRC32 checksums, and torn-write truncation.
- **Idempotency & Limits:** Deduplicates client requests via `(ClientId, RequestId)` and prevents resource starvation via configurable write-set bounds and TTL expiration.

---

## 5. Multi-Raft Sharding & Query Routing Architecture

Phase 8 introduces horizontal sharding and dynamic query routing via Multi-Raft consensus groups:

- **MurmurHash3 Partitioning:** Pure-Java 32-bit MurmurHash3 algorithm deterministically maps keys across shards ($\text{floorMod}(\text{hash}(\text{key}), \text{shardCount})$) with uniform distribution ($\pm 5\%$ deviation).
- **Replication Groups:** Each shard operates as an independent Raft consensus group (`ReplicationGroup`) maintaining isolated logs, terms, and state machines.
- **Fault Isolation:** Leader election, network splits, or slow followers in Shard A have zero operational impact on Shard B.
- **Dynamic Leader Caching:** `LeaderLocator` maintains cached mappings of active shard leaders, intercepting redirect hints and invalidating stale routes on consensus transitions.
- **Client Transparency:** `ShardedAegisDbClient` intercepts single-key and multi-shard operations, routing them transparently to the appropriate shard leader with backoff retry logic.

---

## 6. Distributed Transactions & Two-Phase Commit (2PC) Architecture

Phase 9 establishes atomic cross-shard distributed transactions (Milestone M4 Gate):

- **2PC State Machine:** Explicit transitions (`INIT -> PREPARING -> COMMIT_DECIDED / ABORT_DECIDED -> COMMITTED / ABORTED`) enforced by `DistributedTransactionCoordinator`.
- **Durable Coordinator WAL:** Decisions are durably appended to disk via `DurableCoordinatorLog` using binary framing (`0xAE6120C0` magic header, CRC32 checksums, and synchronous `fsync`).
- **In-Doubt Safety & Prepare Locks:** Shard participants acquire key-level prepare locks during the in-doubt window. No participant commits or aborts unilaterally without coordinator instruction.
- **8-Scenario Crash Recovery Matrix:** `DistributedTransactionRecovery` replays the coordinator log on startup, resolving transactions across all coordinator and participant crash permutations.
- **Optimistic Concurrency Control (OCC):** Read sets are validated at prepare time to ensure serializable execution and eliminate write skew or lost updates across shards.

---

## 7. Chaos Engineering & Security Hardening Architecture

Phase 10 introduces systematic fault injection, continuous safety assertion, and management plane hardening:

- **Composable Fault Injection (`FaultyTransport`):** Decorates the abstract `RaftTransport` layer to introduce deterministic packet drops, latency jitter, duplications, and network partitions without touching core consensus algorithms.
- **Cluster Orchestration (`ChaosOrchestrator`):** Simulates leader kills, follower crashes, split-brain majority/minority partitions, and dynamic network healing.
- **Continuous Invariant Verification (`ChaosInvariantMonitor`):** Concurrently asserts that election safety (at most one leader per term), monotonic terms, log prefix equality, and cross-shard financial balance conservation ($A + B + C = 3000$) remain strictly intact under continuous chaos.
- **Management Plane & RBAC (`ManagementHttpServer`):** Lightweight JDK `HttpServer` with Java 25 virtual threads exposing operational diagnostics. Protected by constant-time Bearer token verification (`MessageDigest.isEqual`) defending against side-channel timing attacks.
- **Security Guardrails:**
  - Key size bounded to $\le 1\text{ KB}$ and payload size bounded to $\le 16\text{ MB}$.
  - Token-bucket rate limiting against Denial-of-Service (DoS).
  - Path traversal sanitization preventing directory escape vulnerabilities.
- **Architectural Rules:** ArchUnit tests (`ChaosArchitectureTest`, `ManagementArchitectureTest`) enforce zero coupling between the consensus/storage/transaction core and the chaos/management subsystems.

---

## 8. Master Architectural Diagrams (Master Plan §25)

The following sequence, deployment, and behavioral diagrams fulfill the formal documentation deliverables required by **Master Project Plan §25**:

### 8.1 Deployment Architecture Diagram
```mermaid
flowchart TB
    subgraph Client Tier
        App[Application Client]
        Admin[Operator / Prometheus Scraper]
    end

    subgraph Cluster Deployment
        subgraph Node 1 - 127.0.0.1
            P1[Raft Transport Port: 9001]
            M1[Management HTTP Port: 9101]
            D1[(Data Dir: /var/lib/aegisdb/node1)]
        end

        subgraph Node 2 - 127.0.0.1
            P2[Raft Transport Port: 9002]
            M2[Management HTTP Port: 9102]
            D2[(Data Dir: /var/lib/aegisdb/node2)]
        end

        subgraph Node 3 - 127.0.0.1
            P3[Raft Transport Port: 9003]
            M3[Management HTTP Port: 9103]
            D3[(Data Dir: /var/lib/aegisdb/node3)]
        end
    end

    App -->|SDK gRPC / In-Memory| P1
    App -.->|Failover Redirect| P2
    Admin -->|GET /metrics Bearer Auth| M1
    Admin -->|GET /health| M2

    P1 <-->|AppendEntries / RequestVote| P2
    P2 <-->|AppendEntries / RequestVote| P3
    P1 <-->|AppendEntries / RequestVote| P3

    P1 --- D1
    P2 --- D2
    P3 --- D3
```

---

### 8.2 Leader Election Sequence Diagram
```mermaid
sequenceDiagram
    autonumber
    participant FollowerA as Node A (Follower)
    participant FollowerB as Node B (Follower)
    participant FollowerC as Node C (Follower)

    Note over FollowerA: Election timeout expires (150-300ms)
    Note over FollowerA: Transition to CANDIDATE<br/>Increment term (Term = 2)<br/>Vote for self (1/3 votes)

    FollowerA->>FollowerB: RequestVote(candidate=NodeA, term=2, lastLogIdx, lastLogTerm)
    FollowerA->>FollowerC: RequestVote(candidate=NodeA, term=2, lastLogIdx, lastLogTerm)

    Note over FollowerB: Term 2 > 1, log up-to-date<br/>Grant vote, reset election timer
    FollowerB-->>FollowerA: RequestVoteResponse(term=2, voteGranted=true)

    Note over FollowerC: Grant vote, reset election timer
    FollowerC-->>FollowerA: RequestVoteResponse(term=2, voteGranted=true)

    Note over FollowerA: Quorum achieved (3/3 votes)<br/>Transition to LEADER

    FollowerA->>FollowerB: AppendEntries(heartbeat, term=2, leaderId=NodeA)
    FollowerA->>FollowerC: AppendEntries(heartbeat, term=2, leaderId=NodeA)

    FollowerB-->>FollowerA: AppendEntriesResponse(term=2, success=true)
    FollowerC-->>FollowerA: AppendEntriesResponse(term=2, success=true)
```

---

### 8.3 Write Replication Sequence Diagram
```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Leader as Node 1 (Leader)
    participant Follower2 as Node 2 (Follower)
    participant Follower3 as Node 3 (Follower)

    Client->>Leader: propose("PUT user:100 Alice")
    Note over Leader: Append to local WAL & RaftLog (Index 10, Term 2)
    Leader->>Follower2: AppendEntries(term=2, prevLogIdx=9, entries=[Idx 10], commitIdx=9)
    Leader->>Follower3: AppendEntries(term=2, prevLogIdx=9, entries=[Idx 10], commitIdx=9)

    Note over Follower2: Validate prevLogIdx 9<br/>Append Idx 10 to WAL
    Follower2-->>Leader: AppendEntriesResponse(term=2, success=true, matchIndex=10)

    Note over Leader: Majority quorum confirmed (2/3 nodes)<br/>Advance commitIndex to 10
    Leader->>Leader: Apply Idx 10 to KeyValueStateMachine
    Leader-->>Client: CompletableFuture.complete(SUCCESS)

    Note over Follower3: Append Idx 10 to WAL
    Follower3-->>Leader: AppendEntriesResponse(term=2, success=true, matchIndex=10)
    Note over Leader: Piggyback commitIndex=10 on next heartbeat
```

---

### 8.4 Crash Recovery Sequence Diagram
```mermaid
sequenceDiagram
    autonumber
    participant Process as Node Process
    participant Storage as WalRecoveryManager
    participant Checkpoint as SnapshotStore
    participant Raft as RaftNode

    Note over Process: Crash / Abrupt Termination Event
    Note over Process: Node Process Restarts

    Process->>Storage: recoverPersistentRaftState()
    Storage-->>Process: Loaded (currentTerm=2, votedFor=Node1)

    Process->>Checkpoint: loadLatestSnapshot()
    Checkpoint-->>Process: Snapshot(lastIncludedIndex=5000, lastIncludedTerm=2, state)

    Process->>Storage: scanWalSegments(fromIndex=5001)
    Note over Storage: Verify magic bytes (0xAE615D80)<br/>Validate CRC32 checksums<br/>Detect torn tail write at end of segment
    Storage->>Storage: Truncate partial uncommitted record at file boundary
    Storage-->>Process: List<RaftLogEntry> (Index 5001 to 5042)

    Process->>Raft: initializeState(snapshotState, replayedEntries)
    Process->>Process: Transition to FOLLOWER / RUNNING
    Process->>Raft: Start transport & synchronize with cluster leader
```

---

### 8.5 Snapshot Installation Sequence Diagram
```mermaid
sequenceDiagram
    autonumber
    participant Leader as Raft Leader
    participant SnapMgr as SnapshotManager
    participant Follower as Lagging Follower

    Note over Leader: Follower nextIndex < leader's earliest log index (compacted)
    Leader->>SnapMgr: openSnapshotStream(snapshotIndex=5000)

    loop For each 64 KB chunk
        SnapMgr->>Follower: InstallSnapshot(term=2, leaderId=L, lastIdx=5000, lastTerm=2, offset, dataChunk, done)
        Note over Follower: Buffer chunk and verify CRC32
        Follower-->>Leader: InstallSnapshotResponse(term=2)
    end

    Note over Follower: All chunks received<br/>Atomically replace KeyValueStateMachine<br/>Update lastIncludedIndex to 5000
    Leader->>Follower: AppendEntries(term=2, prevIdx=5000, entries=[5001..])
```

---

### 8.6 Local MVCC Transaction Sequence Diagram
```mermaid
sequenceDiagram
    autonumber
    participant App as Client Thread
    participant TM as TransactionManager
    participant MVCC as MvccStore
    participant Log as TransactionLog

    App->>TM: beginTransaction(SNAPSHOT_ISOLATION)
    TM->>MVCC: allocateReadTimestamp()
    MVCC-->>TM: StartTimestamp = 105
    TM-->>App: TransactionContext (TxId=1001, StartTs=105)

    App->>TM: get("account:A")
    TM->>MVCC: getVisibleVersion("account:A", readTs=105)
    MVCC-->>App: Value "1000"

    App->>TM: put("account:A", "800")
    Note over TM: Buffer in WriteSet (Read-Your-Own-Writes)
    Note over TM: Acquire eager write lock on "account:A"

    App->>TM: commit()
    Note over TM: Validate first-committer-wins (no newer committed versions > 105)
    TM->>Log: appendCommitRecord(TxId=1001, WriteSet, CRC32)
    TM->>MVCC: applyWrites(WriteSet, commitTs=106)
    TM->>TM: Release write locks
    TM-->>App: Transaction COMMITTED
```

---

### 8.7 Distributed Transaction (2PC) Sequence Diagram
```mermaid
sequenceDiagram
    autonumber
    participant Coord as 2PC Coordinator
    participant CLog as DurableCoordinatorLog
    participant P0 as Participant Shard 0
    participant P1 as Participant Shard 1

    Note over Coord: beginTransaction (TxId=5001)
    Coord->>CLog: logState(TxId=5001, PREPARING)

    par Phase 1: Broadcast PREPARE
        Coord->>P0: prepare(TxId=5001, WriteSet(k0))
        Coord->>P1: prepare(TxId=5001, WriteSet(k1))
    end

    Note over P0: Validate read sets, acquire prepare locks
    P0-->>Coord: PrepareVote.YES
    Note over P1: Validate read sets, acquire prepare locks
    P1-->>Coord: PrepareVote.YES

    Note over Coord: All votes YES -> Decide COMMIT
    Coord->>CLog: logState(TxId=5001, COMMIT_DECIDED, fsync=true)

    par Phase 2: Fan-out COMMIT
        Coord->>P0: commit(TxId=5001)
        Coord->>P1: commit(TxId=5001)
    end

    Note over P0: Apply mutations, release prepare locks
    P0-->>Coord: Ack
    Note over P1: Apply mutations, release prepare locks
    P1-->>Coord: Ack

    Coord->>CLog: logState(TxId=5001, COMMITTED)
```

---

### 8.8 Network Partition & Split-Brain Mitigation Diagram
```mermaid
flowchart TD
    subgraph "Majority Partition (Quorum = 2/3)"
        L1[Node 1 - Active Leader (Term 1)]
        F2[Node 2 - Follower (Term 1)]
        L1 <-->|Heartbeats & Log Replication| F2
        ClientA[Client Traffic] -->|Writes Confirmed| L1
    end

    subgraph "Network Partition Barrier"
        Barrier[X - Inter-node RPCs Blocked - X]
    end

    subgraph "Minority Partition (Quorum = 1/3)"
        F3[Node 3 - Isolated Node]
        F3 -.->|Election timeout fires<br/>Cannot achieve majority| Cand3[Candidate (Term 2)]
        ClientB[Isolated Client Traffic] -.->|Writes BLOCKED / Rejected| F3
    end

    L1 -.->|Blocked| Barrier
    F2 -.->|Blocked| Barrier
    Barrier -.->|Blocked| F3

    classDef majority fill:#d4edda,stroke:#28a745,stroke-width:2px;
    classDef minority fill:#f8d7da,stroke:#dc3545,stroke-width:2px;
    class L1,F2,ClientA majority;
    class F3,Cand3,ClientB minority;
```
