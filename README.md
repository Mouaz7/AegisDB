# AegisDB (JDistDB)

> A Fault Tolerant Distributed Transactional Database Engine in Java

## Sprint 1: Cluster Foundation & Networking

Sprint 1 etablerar grunden för klusterkommunikation och nodhantering enligt specifikationen:
- **US003:** Som operator vill jag kunna starta flera noder.
- **US004:** Som nod vill jag kommunicera med andra noder.

---

### Modularkitektur (Sprint 1)

```text
AegisDB / JDistDB
├── pom.xml                   # Parent POM med Java 25 & gRPC/Protobuf/Testing
├── jdistdb_common/           # Domänmodeller (NodeId, Endpoint, NodeStatus, Configs)
├── jdistdb_protocol/         # Protobuf-kontrakt och domän-RPC (RequestVote, AppendEntries)
├── jdistdb_transport/        # Transportabstraktion (InMemoryTransport, GrpcRaftTransport)
├── jdistdb_node/             # Nodlivscykel (DatabaseNode, NodeBootstrap, NodeLifecycle)
└── jdistdb_integration/      # Acceptanstester för 3-nods kluster (InMemory & gRPC)
```

---

### Uppfyllda Acceptanskriterier (Sprint 1)

| Kriterium | Beskrivning | Status |
|---|---|:---:|
| **Three nodes start** | Tre noder startar parallellt och når status `RUNNING`. | ✅ PASS |
| **Nodes have unique identities** | Varje nod tilldelas unik `NodeId` och dedikerad `Endpoint` (port). | ✅ PASS |
| **Node A can call Node B** | RPC-anrop (`RequestVote`, `AppendEntries`) skickas och besvaras. | ✅ PASS |
| **Timeout works** | Otillgängliga/tysta noder avbryts med `TransportException.timeout`. | ✅ PASS |
| **Errors propagate correctly** | Anrop till stoppade/okända noder propageras som kontrollerade exceptions. | ✅ PASS |
| **Nodes stop gracefully** | Noder stängs ner snyggt, frigör resurser och når status `STOPPED`. | ✅ PASS |

---

### Bygg och Kör Tester

Förutsättningar:
- Java 25
- Maven 3.8+

```bash
mvn clean test
```
