# ADR 0008: Sharding, Multi-Raft Replication Groups, and Dynamic Query Routing

## Status
Accepted

## Context
Following the delivery of single-shard transactions in Phase 7 (ADR 0007), AegisDB requires horizontal scale-out architecture to partition write throughput and state across multiple discrete Raft consensus groups per Master Project Plan §4, §5, §10, §17, §18 & §20 (Phase 8 / US013, US014).

Key requirements:
1. **Multi-Raft Consensus Topology:** Shards must be governed by independent Raft consensus groups (`ReplicationGroup`), preventing a single consensus log from bottlenecking cluster throughput.
2. **Deterministic Uniform Partitioning:** Key space must distribute uniformly across shards using an avalanche-resistant hash function with negative-hash protection.
3. **Dynamic Leader Tracking & Invalidation:** Clients must cache shard leaders, invalidate stale leaders upon failover, and dynamically follow `leaderHint` redirects from non-leaders.
4. **Transparent Query Routing:** The client SDK must route CRUD operations to target shards without requiring developers to manually resolve shard topology.
5. **Shard Fault Isolation:** Leader failures or re-elections in one shard must not affect the operational throughput or latency of other shards.
6. **Clean Architecture:** Keep `aegisdb_sharding` strictly decoupled from Spring, gRPC, and outer management modules per ArchUnit rules (Master Plan §11).

## Decision
1. **Dedicated Module `aegisdb_sharding`**:
   - Created a standalone Maven module depending only on `aegisdb_common` and `aegisdb_raft`.
   - ArchUnit tests enforce zero dependencies on Spring, gRPC, Protobuf, management, benchmark, or chaos modules.

2. **MurmurHash3 Partitioning with `Math.floorMod`**:
   - Implemented pure-Java 32-bit MurmurHash3 with pinned seed `0x9747b28c`.
   - Computed $\text{shardIndex} = \text{floorMod}(\text{hash}(\text{key}), \text{shardCount})$ to guarantee non-negative shard indices without clustering or distribution skew.

3. **Domain Abstractions (`ShardMap`, `ReplicationGroup`, `Shard`)**:
   - `ReplicationGroup` encapsulates member node IDs for a shard's Raft consensus group.
   - `Shard` aggregates `ShardId`, `ShardMetadata`, and `ReplicationGroup`.
   - `ShardMap` provides thread-safe concurrent registration, lookup by ID, and indexed retrieval.

4. **Dynamic Leader Caching (`LeaderLocator` & `DefaultLeaderLocator`)**:
   - Maintains concurrent map of active shard leaders.
   - Updates dynamically when receiving `NotLeaderException(leaderHint)`.
   - Evicts stale leaders upon request timeout or hintless rejection.

5. **Transparent Client Dispatch (`QueryRouter` & `ShardedAegisDbClient`)**:
   - `QueryRouter` resolves target shard via `ShardRouter`, locates the leader via `LeaderLocator`, and dispatches commands with automatic exponential backoff retries.
   - `ShardedAegisDbClient` provides transparent `put`, `get`, and `delete` methods.

## Consequences
### Positive
- **Horizontal Scalability:** Write traffic scales linearly with the number of shards and consensus groups.
- **Fault Isolation:** Shard 0 leader failure has zero impact on Shard 1 or Shard 2.
- **Transparent UX:** Applications use standard key-value APIs without manual shard tracking.
- **Uniform Distribution:** Verified within $\pm 5\%$ of ideal distribution across 30,000+ keys.

### Negative / Trade-offs
- Multi-shard atomic transactions require distributed coordination (addressed in Phase 9 via 2PC).
