# AegisDB Raft Konsensus: Ledarval & Loggreplikering

---

## 1. Översikt

Modulen `aegisdb_raft` implementerar Raft-konsensusprotokollet i Java 25 i enlighet med Ongaro & Ousterhout (§5.1, §5.2, §5.3, §5.4) och kursspecifikationen (Sektion 18, 20, 21, 22, 23, 24, 75, 82, 83 och 107).

Den garanterar stark konsistens och feltolerans genom:
- **Exakt en ledare per term:** Endast en nod kan vinna valet i en given term ($> N/2$ röster).
- **Automatiskt omval vid fel:** Om en ledare kraschar eller nätverket partitioneras upptäcker följarna timeout och väljer en ny ledare.
- **Majoritetsreplikering:** Klientskrivningar committas först när en strikt majoritet ($N/2 + 1$) av noderna har bekräftat posten.
- **Loggkonsistens & konfliktlösning:** Om en följare har divergerande, ocommittade poster trunkeras dessa och ersätts med ledarens auktoritativa poster.
- **Deterministiskt testbar:** Genom `Clock` och `Scheduler`-abstraktioner kan alla val och feltoleransscenarier testas deterministiskt utan slumpmässiga fördröjningar.

---

## 2. Leader Election Sekvensdiagram (Sektion 125, US005)

Nedanstående sekvensdiagram illustrerar ett normalt ledarval i ett 3-noders kluster:

```mermaid
sequenceDiagram
    autonumber
    participant A as Node A (Follower)
    participant B as Node B (Follower)
    participant C as Node C (Follower)

    Note over A: Election timeout utlöses!
    Note over A: Övergår till CANDIDATE<br/>Inkrementerar Term (Term = 1)<br/>Röstar på sig själv (1/3 röster)

    A->>B: RequestVote(candidate=NodeA, term=1)
    A->>C: RequestVote(candidate=NodeA, term=1)

    Note over B: Validerar: Term 1 > 0, ej röstat<br/>Beviljar röst & återställer timer
    B-->>A: RequestVoteResponse(term=1, voteGranted=true)

    Note over C: Validerar: Term 1 > 0, ej röstat<br/>Beviljar röst & återställer timer
    C-->>A: RequestVoteResponse(term=1, voteGranted=true)

    Note over A: Majoritet uppnådd (3/3 röster)!<br/>Övergår till LEADER

    A->>B: AppendEntries(heartbeat, term=1, leader=NodeA)
    A->>C: AppendEntries(heartbeat, term=1, leader=NodeA)

    Note over B,C: Bekräftar ledare & återställer ElectionTimer
    B-->>A: AppendEntriesResponse(term=1, success=true)
    C-->>A: AppendEntriesResponse(term=1, success=true)
```

---

## 3. Log Replication Sekvensdiagram (US006)

Klientpropositionsflöde med majoritetskvittering och framflyttning av commit-index:

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Leader as Node 1 (Leader)
    participant F1 as Node 2 (Follower)
    participant F2 as Node 3 (Follower)

    Client->>Leader: propose("SET account:1 1000")
    Note over Leader: Lägger till i lokal RaftLog (Index 1, Term 1)
    Note over Leader: Broadcast Replication till följare

    Leader->>F1: AppendEntries(term=1, prevIdx=0, entries=[Entry 1], commit=0)
    Leader->>F2: AppendEntries(term=1, prevIdx=0, entries=[Entry 1], commit=0)

    Note over F1: Validerar prevLogIndex 0<br/>Appendar Entry 1 till logg
    F1-->>Leader: AppendEntriesResponse(term=1, success=true, matchIndex=1)

    Note over Leader: 2 av 3 noder har kvitterat (Majoritet uppnådd!)<br/>Flyttar fram commitIndex till 1
    Leader-->>Client: CompletableFuture.complete(commitIndex=1)

    Note over F2: Appendar Entry 1 till logg
    F2-->>Leader: AppendEntriesResponse(term=1, success=true, matchIndex=1)

    Note over Leader: Sänder uppdaterat leaderCommit via nästa replication/heartbeat
    Leader->>F1: AppendEntries(heartbeat, commit=1)
    Leader->>F2: AppendEntries(heartbeat, commit=1)
    Note over F1,F2: Uppdaterar lokal commitIndex till 1
```

---

## 4. Log Conflict Resolution (Ongaro §5.3)

Om en följare har divergerande poster från en tidigare term som aldrig committades:

```mermaid
sequenceDiagram
    autonumber
    participant Leader as Leader (Term 2)
    participant Follower as Follower (Term 1 stale)

    Note over Follower: Logg innehåller [Index 1: Term 1, Index 2: Term 1 (stale)]
    Note over Leader: Logg innehåller [Index 1: Term 1, Index 2: Term 2 (authoritative)]

    Leader->>Follower: AppendEntries(prevIdx=2, prevTerm=2, entries=[...])
    Note over Follower: LogConflictResolver: Term-mismatch på index 2 (1 != 2)!
    Follower-->>Leader: AppendEntriesResponse(success=false, matchIndex=1)

    Note over Leader: ReplicationManager backar nextIndex till 2
    Leader->>Follower: AppendEntries(prevIdx=1, prevTerm=1, entries=[Index 2: Term 2])
    Note over Follower: LogConflictResolver: prevLog matchar!<br/>Trunkerar felaktiga poster från index 2<br/>Appendar ledarens auktoritativa post
    Follower-->>Leader: AppendEntriesResponse(success=true, matchIndex=2)
```

---

## 5. Tillståndsmaskin för Roller

```mermaid
stateDiagram-v2
    [*] --> FOLLOWER: Uppstart
    FOLLOWER --> CANDIDATE: Election Timeout utlöses
    CANDIDATE --> LEADER: Erhåller röster från majoritet (> N/2)
    CANDIDATE --> CANDIDATE: Ny Election Timeout (Split vote / ingen majoritet)
    CANDIDATE --> FOLLOWER: Upptäcker giltig ledare eller högre term
    LEADER --> FOLLOWER: Upptäcker högre term hos peer
```

---

## 6. Invarianter som aldrig får brytas (Sektion 20 & 75)

1. **At most one leader per term:** Högst en ledare får någonsin väljas i en given term.
2. **Terms never decrease:** En nods term får aldrig minska i värde.
3. **At most one vote per term:** En nod röstar högst en gång per term.
4. **Committed entries are never overwritten:** En post som har committats av en majoritet får aldrig trunkeras eller skrivas över.
5. **Committed entries appear in identical order:** Samtliga klusternoder har identisk sekvens av poster upp till commit-index.
