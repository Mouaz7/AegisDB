# AegisDB Sharding and Routing Architecture

**Plan Version:** 1.0 | September 2026  
**Module:** `aegisdb_sharding` (with `aegisdb_common`, `aegisdb_client`, and `aegisdb_integration`)  
**User Stories:** US013, US014 | Master Project Plan §4, §5, §10, §17, §18

---

## 1. Executive Summary

AegisDB supports horizontal scale-out and data distribution through a **Multi-Raft Sharding Architecture**. The database key space is partitioned across discrete, independent shards. Each shard is governed by its own Raft consensus group (`ReplicationGroup`), ensuring that write volume and consensus traffic are partitioned rather than bottlenecked on a single cluster-wide log.

Client applications interface with a unified, transparent SDK (`ShardedAegisDbClient` and `QueryRouter`) which hashes keys, resolves shard topology via `ShardRouter`, locates active shard leaders via `LeaderLocator`, and transparently recovers from leader re-elections.

```
                         Client Application
                                 │
                                 ▼
                       ShardedAegisDbClient
                                 │
                                 ▼
                            QueryRouter
                                 │
                ┌────────────────┴────────────────┐
                ▼                                 ▼
           ShardRouter                      LeaderLocator
       (HashPartitioner)                  (Cached Leaders)
                │                                 │
     ┌──────────┴──────────┐                      │
     ▼                     ▼                      │
  Shard 0               Shard 1                   │
 (shard-0)             (shard-1)                  │
     │                     │                      │
     ▼                     ▼                      │
Raft Group 0          Raft Group 1 ◄──────────────┘
  (N1, N2, N3)          (N4, N5, N6)
```

---

## 2. Partitioning Algorithm & Math

### 2.1 Formula
Partition assignment is computed deterministically:

$$\text{shardIndex} = \text{floorMod}(\text{Murmur3\_32}(\text{key}, \text{seed}), \text{shardCount})$$

$$\text{destinationShard} = \text{shardMap}.\text{getShardByIndex}(\text{shardIndex})$$

### 2.2 Why 32-bit MurmurHash3?
1. **Uniform Dispersion:** MurmurHash3 provides excellent avalanche characteristics. Flipping a single bit in the key results in a completely pseudo-random 32-bit hash, preventing data clustering.
2. **Deterministic Cross-Platform Behavior:** Pure-Java implementation with pinned seed `0x9747b28c` ensures identical routing across any JVM, OS, or architecture.
3. **Negative Hash Protection:** Java standard `%` operator can return negative values for negative integers. AegisDB strictly enforces `Math.floorMod(hash, shardCount)` to guarantee non-negative indices in $[0, \text{shardCount}-1]$.

---

## 3. Core Components Inventory

| Class / Record | Package | Responsibility |
| :--- | :--- | :--- |
| `ShardId` | `se.mouaz.aegisdb.common` | Strongly-typed, immutable shard identifier record (`shard-0`, `shard-1`). |
| `ShardMetadata` | `se.mouaz.aegisdb.sharding` | Immutable metadata descriptor (creation time, replica count, shard status). |
| `ReplicationGroup` | `se.mouaz.aegisdb.sharding` | Consensus replica set descriptor holding `ShardId` and `Set<NodeId>` member nodes. |
| `Shard` | `se.mouaz.aegisdb.sharding` | Logical partition aggregate combining `ShardId`, `ShardMetadata`, and `ReplicationGroup`. |
| `Partitioner` | `se.mouaz.aegisdb.sharding` | Strategy interface: `ShardId selectShard(String key, ShardMap shardMap)`. |
| `HashPartitioner` | `se.mouaz.aegisdb.sharding` | Murmur3-based implementation of `Partitioner` using `floorMod`. |
| `ShardMap` | `se.mouaz.aegisdb.sharding` | Thread-safe topology map and registry of active shards. |
| `LeaderLocator` | `se.mouaz.aegisdb.sharding` | Interface for tracking, caching, and invalidating active shard leaders. |
| `DefaultLeaderLocator` | `se.mouaz.aegisdb.sharding` | Production implementation with concurrent caching, listener callbacks, and stale checks. |
| `ShardRouter` | `se.mouaz.aegisdb.sharding` | Binds `Partitioner` and `ShardMap` to route keys to `Shard` and `ReplicationGroup`. |
| `QueryRouter` | `se.mouaz.aegisdb.sharding` | Dispatches operations to shard leaders with automatic redirect handling and backoff retries. |
| `ShardManager` | `se.mouaz.aegisdb.sharding` | Operational coordinator for static shard initialization and topology administration. |
| `ShardedAegisDbClient` | `se.mouaz.aegisdb.client` | High-level client SDK implementing `AegisDbClient` backed by `QueryRouter`. |

---

## 4. Query Routing & Transparent Failover

When a client initiates an operation (e.g. `client.putString("user:42", "data")`):

1. **Shard Resolution:** `QueryRouter` asks `ShardRouter` for the destination shard:
   $$\text{ShardId} = \text{HashPartitioner}.\text{selectShard}(\text{key}, \text{ShardMap})$$
2. **Leader Lookup:** `QueryRouter` queries `LeaderLocator` for the cached leader of that shard.
3. **Dispatch:**
   - If leader is cached, the request is dispatched directly to that node.
   - If leader is unknown, the request is dispatched to a round-robin member of the shard's `ReplicationGroup`.
4. **Redirect Handling:**
   - If the target node is not the leader, it replies with `NotLeaderException(leaderHint)`.
   - If `leaderHint` is present, `LeaderLocator.updateLeader(shardId, leaderHint)` updates the cache.
   - If `leaderHint` is absent, `LeaderLocator.invalidateLeader(shardId, targetNode)` evicts the stale entry.
   - The query router automatically retries on the newly discovered leader using exponential backoff.

---

## 5. Multi-Raft Safety & Cluster-Scoped Invariants

In a multi-shard architecture, each shard operates as an independent consensus group.
To ensure mathematical safety without false positives:
- **Election Safety:** `RaftInvariants.recordLeaderElected(clusterId, term, leaderId)` verifies at most one leader per term *within each consensus group*.
- **Committed Entries Invariant:** `RaftInvariants.assertCommittedEntriesNeverOverwritten(clusterId, commitIndex, log)` guarantees prefix consistency per group without index collisions between separate shards.

---

## 6. Single-Shard Transactions & 2PC Roadmap

In Phase 8:
- Single-shard transactions can be executed on any individual shard via `client.beginTransaction(shardId, level)`.
- If an application attempts cross-shard operations within a single transaction, `ShardedAegisDbClient` safely guards and rejects the operation:
  > *"Cross-shard distributed transactions require Two-Phase Commit (Phase 9)."*
- In **Phase 9**, the `DistributedTransactionCoordinator` will build on this sharding foundation to coordinate atomic cross-shard commits using Two-Phase Commit (2PC).
