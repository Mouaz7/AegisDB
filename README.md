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
├── aegisdb_raft/             # Raft Consensus Engine (Election, Heartbeats, State, Invariants)
├── aegisdb_node/             # Nodlivscykel (DatabaseNode, NodeBootstrap, NodeLifecycle)
└── aegisdb_integration/      # Acceptanstester & verifieringsdemos för Sprint 1 och 2
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

### Kör live-demonstration för Sprint 1
```bash
./scripts/run-sprint1-demo.sh
```

### Kör live-demonstration för Sprint 2 (Raft Leader Election)
```bash
./scripts/run-sprint2-demo.sh
```
