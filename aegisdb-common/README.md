# AegisDB Common (`aegisdb-common`)

`aegisdb-common` serves as the foundational shared kernel of the AegisDB distributed database. It contains core domain value objects, identifiers, result monads, and configuration contracts shared across the entire system.

## Design Philosophy

- **Zero Coupling**: Free of external runtime dependencies (except standard SLF4J logging API).
- **No Dumping Ground**: Business logic, protocol definitions (gRPC/Protobuf), consensus logic, and persistence routines are strictly forbidden from residing in this module.
- **Immutability**: Domain models are implemented as immutable records or thread-safe POJOs.

## Public Stable Domain API

These types represent the public domain vocabulary of AegisDB and are guaranteed to maintain backwards compatibility across minor releases:

| Type | Classification | Description |
|---|---|---|
| `NodeId` | Public Stable | Strongly typed unique identifier for a cluster node. |
| `ClusterId` | Public Stable | Strongly typed identifier for the logical AegisDB cluster. |
| `ShardId` | Public Stable | Partition identifier for multi-raft horizontal sharding. |
| `TransactionId` | Public Stable | Monotonically generated logical transaction identifier. |
| `ClientId` | Public Stable | Identification token for external client connections. |
| `RequestId` | Public Stable | Correlation identifier for request-response RPC tracking. |
| `Endpoint` | Public Stable | Host, port, and scheme tuple representing network addressability. |
| `Result<T, E>` | Public Stable | Functional result type for error handling without exceptions. |
| `ErrorCode` | Public Stable | Standardized diagnostic error taxonomy for engine failures. |
| `DatabaseException` | Public Stable | Root runtime exception hierarchy for AegisDB operations. |

## Internal & Operational Models

These types govern internal node state, topologies, and bootstrap configuration:

| Type | Classification | Description |
|---|---|---|
| `ClusterConfiguration` | Internal | Static topology and discovery descriptors for cluster members. |
| `NodeConfiguration` | Internal | Local node binding, storage directory, and identity settings. |
| `NetworkConfiguration` | Internal | Timeout bounds, heartbeat intervals, and socket buffer sizes. |
| `NodeStatus` | Internal | Lifecycle state machine enumeration (`STARTING`, `RUNNING`, `STOPPING`, `STOPPED`). |
