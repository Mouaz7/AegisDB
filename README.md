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
├── aegisdb_node/             # Nodlivscykel (DatabaseNode, NodeBootstrap, NodeLifecycle)
└── aegisdb_integration/      # Acceptanstester för 3-nods kluster & verifieringsdemo
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

## Bygg och Kör

### Förutsättningar
- **Java 25 LTS**
- **Maven 3.8+**

### Kör alla enhets- och integrationstester
```bash
mvn clean test
```

### Kör live-demonstration för Sprint 1
```bash
mvn exec:java -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint1Demo \
    -Dexec.classpathScope=test
```
Eller via skript:
```bash
./scripts/run-sprint1-demo.sh
```
