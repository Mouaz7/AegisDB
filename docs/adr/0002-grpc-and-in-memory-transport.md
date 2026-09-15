# ADR 0002: gRPC vs Raw TCP and In-Memory Test Transport

## Status
Accepted

## Context
A distributed consensus cluster requires high-performance, strongly-typed network communication between peers for `RequestVote`, `AppendEntries`, and `InstallSnapshot` RPCs (Master Plan §2, §5, §6). Distributed testing often suffers from flakiness, port collision, and socket overhead when tests rely strictly on physical network sockets.

Key requirements:
1. Strongly-typed, versionable binary wire contracts.
2. Production-grade RPC implementation with streaming and chunking support.
3. Testability without opening network sockets or invoking external daemon processes.
4. Clean abstraction boundaries: consensus logic must never couple to transport implementation details (Master Plan §4).

## Decision
1. **Protocol Buffers (.proto) over gRPC for Network Transport**:
   - `aegisdb-protocol` defines canonical Protobuf contracts (`raft_rpc.proto`).
   - `GrpcRaftTransport` provides high-throughput production networking using Netty-shaded gRPC with keepalives and channel pooling.
2. **Pluggable Transport Abstraction (`RaftTransport`)**:
   - Core consensus logic in `aegisdb-raft` interacts only with the `RaftTransport` interface, never with gRPC directly.
3. **Deterministic `InMemoryTransport`**:
   - For fast, deterministic unit, property, and integration tests, `InMemoryTransport` enables multi-node clusters within a single JVM without network sockets or port allocations.
   - Supports fault injection (`FaultyTransport`) for simulated message delays, packet drops, and network partitions.

## Consequences
### Positive
- Strict separation of consensus algorithm from networking protocols (enforced by ArchUnit).
- Millisecond-fast multi-node integration test runs in memory.
- Schema evolution and binary serialization efficiency via Protocol Buffers.

### Negative
- Requires maintaining both `GrpcRaftTransport` and `InMemoryTransport` adapters.
