# AegisDB Raft Konsensus & Ledarval (Leader Election)

---

## 1. Översikt

Modulen `aegisdb_raft` implementerar Raft-konsensusprotokollet för ledarval (Sprint 2) och loggreplikering (Sprint 3). Den garanterar stark konsistens och feltolerans genom följande egenskaper:
- **Exakt en ledare per term:** Endast en nod kan vinna valet i en given term ($> N/2$ röster).
- **Automatiskt omval vid fel:** Om en ledare kraschar eller nätverket partitioneras upptäcker följarna timeout och väljer en ny ledare.
- **Deterministiskt testbar:** Genom `Clock` och `Scheduler`-abstraktioner kan alla val och feltoleransscenarier testas deterministiskt utan slumpmässiga fördröjningar.

---

## 2. Leader Election Sekvensdiagram (Sektion 125)

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

## 3. Tillståndsmaskin för Roller

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

## 4. Invarianter som aldrig får brytas (Sektion 20 & 75)

1. **At most one leader per term:** Högst en ledare får någonsin väljas i en given term.
2. **Terms never decrease:** En nods term får aldrig minska i värde.
3. **At most one vote per term:** En nod röstar högst en gång per term.
