# AegisDB Raft Consensus: Leader Election & Log Replication

---

## 1. Overview

The `aegisdb-raft` module implements the Raft consensus protocol in Java 21 in accordance with Ongaro & Ousterhout (§5.1, §5.2, §5.3, §5.4) and the course specification (Sections 18, 20, 21, 22, 23, 24, 75, 82, 83, and 107).

It guarantees strong consistency and fault tolerance through:
- **Exactly one leader per term:** Only one node can win the election in any given term ($> N/2$ votes).
- **Automatic failover:** If a leader crashes or the network partitions, followers detect timeout and safely elect a new leader.
- **Majority replication:** Client writes are committed only after a strict majority ($N/2 + 1$) of nodes acknowledge the entry.
- **Log consistency & conflict repair:** If a follower has diverging, uncommitted entries, they are automatically truncated and replaced with authoritative leader entries.
- **Deterministically testable:** Through `Clock` and `Scheduler` abstractions, all elections and fault scenarios are testable deterministically without unpredictable sleeps.

---

## 2. Leader Election Sequence Diagram (Section 125, US005)

The sequence diagram below illustrates a normal leader election in a 3-node cluster:

```mermaid
sequenceDiagram
    autonumber
    participant A as Node A (Follower)
    participant B as Node B (Follower)
    participant C as Node C (Follower)

    Note over A: Election timeout fires!
    Note over A: Transitions to CANDIDATE<br/>Increments Term (Term = 1)<br/>Votes for self (1/3 votes)

    A->>B: RequestVote(candidate=NodeA, term=1)
    A->>C: RequestVote(candidate=NodeA, term=1)

    Note over B: Validates: Term 1 > 0, not voted<br/>Grants vote & resets election timer
    B-->>A: RequestVoteResponse(term=1, voteGranted=true)

    Note over C: Validates: Term 1 > 0, not voted<br/>Grants vote & resets election timer
    C-->>A: RequestVoteResponse(term=1, voteGranted=true)

    Note over A: Quorum reached (3/3 votes)!<br/>Transitions to LEADER

    A->>B: AppendEntries(heartbeat, term=1, leader=NodeA)
    A->>C: AppendEntries(heartbeat, term=1, leader=NodeA)

    Note over B,C: Confirms leader & resets ElectionTimer
    B-->>A: AppendEntriesResponse(term=1, success=true)
    C-->>A: AppendEntriesResponse(term=1, success=true)
```

---

## 3. Log Replication Sequence Diagram (US006)

Client proposal workflow with majority acknowledgment and commit index advancement:

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Leader as Node 1 (Leader)
    participant F1 as Node 2 (Follower)
    participant F2 as Node 3 (Follower)

    Client->>Leader: propose("SET account:1 1000")
    Note over Leader: Appends to local RaftLog (Index 1, Term 1)
    Note over Leader: Broadcasts replication to followers

    Leader->>F1: AppendEntries(term=1, prevIdx=0, entries=[Entry 1], commit=0)
    Leader->>F2: AppendEntries(term=1, prevIdx=0, entries=[Entry 1], commit=0)

    Note over F1: Validates prevLogIndex 0<br/>Appends Entry 1 to log
    F1-->>Leader: AppendEntriesResponse(term=1, success=true, matchIndex=1)

    Note over Leader: 2 of 3 nodes acknowledged (Majority reached!)<br/>Advances commitIndex to 1
    Leader-->>Client: CompletableFuture.complete(commitIndex=1)

    Note over F2: Appends Entry 1 to log
    F2-->>Leader: AppendEntriesResponse(term=1, success=true, matchIndex=1)

    Note over Leader: Sends updated leaderCommit via next replication/heartbeat
    Leader->>F1: AppendEntries(heartbeat, commit=1)
    Leader->>F2: AppendEntries(heartbeat, commit=1)
    Note over F1,F2: Updates local commitIndex to 1
```

---

## 4. Log Conflict Resolution (Ongaro §5.3)

Handling a follower with diverging, uncommitted entries from a previous term:

```mermaid
sequenceDiagram
    autonumber
    participant Leader as Leader (Term 2)
    participant Follower as Follower (Term 1 stale)

    Note over Follower: Log contains [Index 1: Term 1, Index 2: Term 1 (stale)]
    Note over Leader: Log contains [Index 1: Term 1, Index 2: Term 2 (authoritative)]

    Leader->>Follower: AppendEntries(prevIdx=2, prevTerm=2, entries=[...])
    Note over Follower: LogConflictResolver: Term mismatch at index 2 (1 != 2)!
    Follower-->>Leader: AppendEntriesResponse(success=false, matchIndex=1)

    Note over Leader: ReplicationManager decrements nextIndex to 2
    Leader->>Follower: AppendEntries(prevIdx=1, prevTerm=1, entries=[Index 2: Term 2])
    Note over Follower: LogConflictResolver: prevLog matches!<br/>Truncates invalid entries from index 2<br/>Appends authoritative leader entry
    Follower-->>Leader: AppendEntriesResponse(success=true, matchIndex=2)
```

---

## 5. Node Role State Machine

```mermaid
stateDiagram-v2
    [*] --> FOLLOWER: Startup
    FOLLOWER --> CANDIDATE: Election Timeout fires
    CANDIDATE --> LEADER: Receives votes from majority (> N/2)
    CANDIDATE --> CANDIDATE: New Election Timeout (Split vote / no quorum)
    CANDIDATE --> FOLLOWER: Discovers valid leader or higher term
    LEADER --> FOLLOWER: Discovers peer with higher term
```

---

## 6. Strict Raft Invariants (Sections 20 & 75)

1. **At most one leader per term:** At most one leader may ever be elected in any given term.
2. **Terms never decrease:** A node's term must never decrease in value.
3. **At most one vote per term:** A node votes at most once per term.
4. **Committed entries are never overwritten:** An entry committed by a majority must never be truncated or overwritten.
5. **Committed entries appear in identical order:** All cluster nodes share an identical sequence of committed entries up to commit-index.
