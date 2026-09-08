# AegisDB

> **A Fault Tolerant Distributed Transactional Database Engine in Java**

AegisDB är en distribuerad transaktionsdatabas byggd från grunden i Java 25. Projektet implementerar Raft-konsensus för stark konsistens, feltolerant Write-Ahead Logging (WAL) och snapshots för persistent kraschåterställning, MVCC för isolerade transaktioner samt horisontell sharding med Two-Phase Commit (2PC).

---

## Modulstruktur

Fil- och modulstrukturen är enhetligt anpassad till repositorynamnet **AegisDB**:

```text
AegisDB/
├── pom.xml                   # Parent POM (Java 25, gRPC, Protobuf, JUnit 5)
├── README.md                 # Projektdokumentation & instruktioner
├── LICENSE                   # MIT License
├── docker/                   # Docker- och container-konfigurationer
├── docs/                     # Arkitektur, garantier & felmodeller
│   ├── architecture.md       # Systemarkitektur & lagerseparation
│   ├── failure_model.md      # Felmodell (fail-stop, crash-recovery, nätverk)
│   └── consistency.md        # Konsistensmodell (Linearizability, Snapshot Isolation)
├── scripts/                  # Skript för byggning och testning
├── experiments/              # Prestandamätningar och benchmarks
│
├── aegisdb_common/           # Domänmodeller (NodeId, Endpoint, NodeStatus, Configs)
├── aegisdb_protocol/         # Protobuf-kontrakt och gRPC RPC-definitioner
├── aegisdb_transport/        # Transportabstraktion (InMemoryTransport, GrpcRaftTransport)
├── aegisdb_raft/             # Raft Consensus Engine (Election, Heartbeats, State, Invariants, Snapshots)
├── aegisdb_storage/          # Disk-persistens, WAL, CRC32 Checksums, Snapshots & Återställning
├── aegisdb_node/             # Nodlivscykel (DatabaseNode, NodeBootstrap, NodeLifecycle)
├── aegisdb_client/           # Java SDK Client (AegisDbClient, transparent redirect/retry)
└── aegisdb_integration/      # Acceptanstester & verifieringsdemos för Sprint 1-5
```

---

## Sprint 1: Cluster Foundation & Networking

Sprint 1 etablerar grunden för klusterkommunikation och nodhantering:
- **US003:** Som operator vill jag kunna starta flera noder.
- **US004:** Som nod vill jag kommunicera med andra noder.

### Uppfyllda Acceptanskriterier (Sprint 1)

| Kriterium | Beskrivning | Status |
|---|---|:---:|
| **Three nodes start** | Tre noder startar parallellt och når status `RUNNING`. | ✅ PASS |
| **Nodes have unique identities** | Varje nod tilldelas unik `NodeId` och dedikerad `Endpoint` (port). | ✅ PASS |
| **Node A can call Node B** | RPC-anrop (`RequestVote`, `AppendEntries`) skickas och besvaras via gRPC & InMemory. | ✅ PASS |
| **Timeout works** | Otillgängliga eller tysta noder avbryts med kontrollerad timeout. | ✅ PASS |
| **Errors propagate correctly** | Anrop till stoppade/okända noder propageras som `TransportException`. | ✅ PASS |
| **Nodes stop gracefully** | Noder stängs ner snyggt, frigör resurser och når status `STOPPED`. | ✅ PASS |

---

## Sprint 2: Raft Consensus Engine - Leader Election

Sprint 2 implementerar Raft konsensusmotorns ledarval (US005) med single-threaded event loop, randomiserade timeouts och strikta säkerhetsinvarianter:
- **US005:** Som kluster vill vi utse en ledare via Raft leader election.

### Uppfyllda Acceptanskriterier (Sprint 2)

| Kriterium | Beskrivning | Status |
|---|---|:---:|
| **Exactly one leader exists per term** | Högst en ledare väljs per term (Election Safety Invariant). Val kräver absolut majoritet (quorum: `N/2 + 1`). | ✅ PASS |
| **Followers reset timeout after valid heartbeat** | Ledaren sänder regelbundna hjärtslag (`AppendEntries`). Följare nollställer sin election timer och förblir stabila följare. | ✅ PASS |
| **New leader is elected after failure** | När en ledare kraschar upptäcker kvarvarande noder timeout och väljer säkert en ny ledare med ökad term. | ✅ PASS |

### Arkitektur & Invarianter (Sprint 2)
- **Single-Threaded Event Loop**: Alla tillståndsförändringar körs via en sekventiell händelsekö (`Executors.newSingleThreadExecutor`) utan samtidiga race conditions (Section 107).
- **Tidshanteringsabstraktion**: `Clock` (`SystemClock`, `TestClock`) och `Scheduler` (`SystemScheduler`, `DeterministicScheduler`) möjliggör deterministiska simuleringstester utan osäkra sleeps (Section 109 & 141).
- **Strikt isolering**: ArchUnit-arkitekturtest verifierar att `aegisdb_raft` aldrig beror på Spring, management, benchmarks eller kaosmoduler (Section 105).

---

## Sprint 3: Raft Consensus Engine - Log Replication

Sprint 3 implementerar Raft-loggreplikering (US006) enligt Ongaro §5.3/§5.4 med strikt majoritetskvittering, commit-index-hantering och automatisk konfliktlösning:
- **US006:** Som leader vill jag replikera writes till majority innan commit.

### Uppfyllda Acceptanskriterier (Sprint 3)

| Kriterium | Beskrivning | Status |
|---|---|:---:|
| **Writes replicate** | Ledaren tar emot klientskrivningar (`propose`), appendar till lokal `RaftLog` och sänder ut poster till följare. | ✅ PASS |
| **Majority required** | Skrivningar committas först när en strikt majoritet ($N/2 + 1$) av noderna har kvitterat posten. Vid minoritetsisolation blockeras commit säkert. | ✅ PASS |
| **CommitIndex advances correctly** | `commitIndex` ökar monotont enligt Ongaro §5.3/§5.4 (ledare committar poster från innevarande term). | ✅ PASS |
| **Follower catches up** | En frånkopplad följare som återansluter mottar automatiskt alla saknade poster och synkroniseras upp till ledarens `commitIndex`. | ✅ PASS |
| **Conflicting entries are repaired** | Divergerande, ocommittade poster i en följares logg trunkeras automatiskt och ersätts med ledarens auktoritativa poster via `LogConflictResolver`. | ✅ PASS |

### Arkitektur & Säkerhetsinvarianter (Sprint 3)
- **1-baserad loggindexering**: `RaftLog` använder 1-baserat sekvensnummer med sentinel-post på index 0.
- **Single-Threaded Event Loop**: Klientskrivningar (`ClientWriteEvent`) och replikeringssvar (`AppendEntriesResponseEvent`) bearbetas uteslutande sekventiellt utan samtidiga lås.
- **Section 75 Raft Invariants**: Automatiskt verifierade i körtid och tester:
  - *Committed entries are never overwritten*
  - *Committed entries appear in identical order across all nodes*

---

## Sprint 4: Persistence and Recovery

Sprint 4 implementerar lokal feltolerant persistens och kraschåterställning enligt Master Project Plan §8 & §17 (US007 & US008):
- **US007:** Som databas vill jag att committad data överlever krascher.
- **US008:** Som Raft-nod vill jag att term/votedFor är beständig över omstarter.

### Uppfyllda Acceptanskriterier (Sprint 4)

| Kriterium | Beskrivning | Status |
|---|---|:---:|
| **WAL segments and checksums** | Segmentfiler (`wal-00000000000000000001.seg`) med automatisk rollover vid segmentgräns och CRC32-checksummor över headrar och payloads. | ✅ PASS |
| **Flush/fsync policy** | Konfigurerbar synkroniseringspolicy (`ALWAYS`, `PERIODIC`, `MANUAL`). `ALWAYS` garanterar fysisk diskpersistens via `force(true)` innan skrivning bekräftas. | ✅ PASS |
| **Persistent term/votedFor** | `currentTerm` och `votedFor` sparas atomärt med temp-fil + `fsync` + `ATOMIC_MOVE` före RPC-svar (§5.2). | ✅ PASS |
| **Partial-write recovery** | Avbrutna skrivningar vid filslutet (torn tail) upptäcks automatiskt vid uppstart och trunkeras säkert till senaste intakta post. | ✅ PASS |
| **Corruption detection** | Bit-flippar, ogiltigt magic number eller felaktig CRC32 i existerande poster flaggas omedelbart med `CorruptedWalException`. | ✅ PASS |
| **Restart tests** | 3-nods kluster överlever abrupt processdöd/krasch. Samtliga noder återställer term, röster och loggsekvens och fortsätter konsensus. | ✅ PASS |

### Arkitektur & Säkerhet (Sprint 4)
- **DurableRaftLog**: Ersätter och utökar `RaftLog` med write-through till WAL och synkron `fsync` före minnesuppdatering.
- **Path Traversal Protection**: Säkerhetsvalidering av datakataloger förhindrar otillåtna relativa sökvägar.
- **Bounded Allocation Limits**: Maximal poststorlek begränsad till 16 MB för att förhindra minnesutmattningsattacker vid manipulerade headrar.
- **Zero-Dependency Architecture**: `aegisdb_storage` har noll beroenden till Spring och gRPC.

---

## Sprint 5: Snapshots and Replicated Key-Value Store (Milestone M2 Gate)

Sprint 5 implementerar loggkompaktering via snapshots, InstallSnapshot RPC, en replikerad Key-Value State Machine samt klient-SDK (US009 & US010; Milestone M2 Gate):
- **US009:** Som databasnod vill jag ha snapshots så loggen inte växer obegränsat.
- **US010:** Som klient vill jag ha replikerade nyckel-värde operationer.

### Uppfyllda Acceptanskriterier (Sprint 5)

| Kriterium | Beskrivning | Status |
|---|---|:---:|
| **Snapshot metadata and checksum** | Punkt-i-tid snapshots (`snapshot-%020d-%020d.snap`) med Magic `0xAE615DA2`, version, framing och CRC32-checksummor över datat. Atomär `.tmp` + `ATOMIC_MOVE` och retention på 2 senaste snapshots. | ✅ PASS |
| **Snapshot install** | Chunkad `InstallSnapshot` RPC över gRPC & InMemoryTransport med 64 KB chunking, offset, done-flagga och checksum-kontroll. | ✅ PASS |
| **KeyValueStateMachine** | Trådsäker `ConcurrentSkipListMap`-baserad state machine med stöd för PUT, GET, DELETE samt deterministisk snapshot serialisering och återställning. | ✅ PASS |
| **Client PUT/GET/DELETE** | Dedikerad Java SDK-modul `aegisdb_client` med `AegisDbClient` och `DefaultAegisDbClient`. Hanterar automatisk ledarupptäckt och transparent redirect vid `NotLeaderException`. | ✅ PASS |
| **Follower catch-up from snapshot** | Eftersläpande eller frånkopplade noder vars saknade loggposter har kompakterats bort hämtas automatiskt in via `InstallSnapshot`. | ✅ PASS |
| **Milestone M2 Gate** | 3-nods persistent replikerat nyckel-värde-kluster överlever ledarhaveri, genomför säkert omval, och låter klienter fortsätta läsa och skriva utan dataförlust. | ✅ PASS |

---

## Bygg och Kör

### Förutsättningar
- **Java 25 LTS**
- **Maven 3.8+**

### Kör alla enhets-, arkitektur- och integrationstester
```bash
mvn clean test
```
eller via skript:
```bash
./scripts/test-all.sh
```

### Kör live-demonstration för Sprint 1 (Nätverk & Noder)
```bash
./scripts/run-sprint1-demo.sh
```

### Kör live-demonstration för Sprint 2 (Raft Ledarval)
```bash
./scripts/run-sprint2-demo.sh
```

### Kör live-demonstration för Sprint 3 (Raft Loggreplikering)
```bash
./scripts/run-sprint3-demo.sh
```

### Kör live-demonstration för Sprint 4 (Persistens & Kraschåterställning)
```bash
./scripts/run-sprint4-demo.sh
```

### Kör live-demonstration för Sprint 5 (Snapshots & Replicated KV Store / Milestone M2)
```bash
./scripts/run-sprint5-demo.sh
```


