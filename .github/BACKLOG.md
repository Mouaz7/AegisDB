# AegisDB Engineering Backlog & Post-Stabilization Roadmap

This backlog documents prioritized technical enhancements and research milestones planned for AegisDB following core stabilization. These issues can be created in the GitHub repository manually or automatically using `scripts/create-github-issues.sh`.

---

### Issue 1: Jepsen-Style Black-Box Verification Harness

**Labels:** `correctness`, `testing`, `chaos`, `research`

#### Problem Statement
While unit, integration, and in-memory chaos tests (`aegisdb-chaos`) validate isolated safety invariants, AegisDB lacks external black-box linearizability and sequential consistency verification across true OS process boundaries under network partitions and packet loss.

#### Proposed Solution
1. Implement a client-side verification harness modeled after Jepsen (using Knossos or Porcupine algorithms).
2. Execute concurrent independent client threads performing randomized transactional registers (`read`, `write`, `cas`).
3. Inject real network chaos (iptables partition, tc netem packet drop/delay) across multi-process clusters.
4. Verify linearizability history graphs to prove zero stale reads or lost committed writes during cluster partition and leader failover.

#### Acceptance Criteria
- [ ] Automated CLI runner executes against a 3-node or 5-node cluster.
- [ ] Concurrent history verification completes with 0 linearizability violations across 10,000+ operations.
- [ ] Automated HTML/SVG artifact output detailing the execution history graph.

---

### Issue 2: Cross-Shard Distributed Transaction Isolation Hardening (Serializable 2PC)

**Labels:** `transactions`, `distributed`, `isolation`

#### Problem Statement
Currently, Two-Phase Commit (`DistributedTransactionCoordinator`) guarantees atomic distributed commit/abort across shards. However, multi-shard transactions execute under independent shard snapshot timestamps, which does not guarantee global strict serializability or prevent cross-shard write skew without global timestamp synchronization.

#### Proposed Solution
1. Implement cross-shard read-set anti-dependency checking during the 2PC `PREPARE` phase.
2. Introduce a monotonic Logical/Hybrid Logical Clock (HLC) or centralized timestamp allocator for global snapshot ordering.
3. Validate multi-shard read sets across all participant shards before issuing `GLOBAL_COMMIT`.
4. Add comprehensive multi-shard write skew and cross-shard anomaly tests.

#### Acceptance Criteria
- [ ] Multi-shard transactions validate participant read versions during `PREPARE`.
- [ ] Anti-dependency conflict on any participant causes coordinated `GLOBAL_ABORT`.
- [ ] Cross-shard transfer tests preserve multi-shard conservation invariants under concurrent stress.

---

### Issue 3: Dynamic Re-Sharding and Non-Blocking Partition Rebalancing without Downtime

**Labels:** `sharding`, `scalability`, `routing`

#### Problem Statement
Currently, shard topology and virtual node assignments (`ConsistentHashRouter`) are statically configured at cluster startup. Adding or removing a shard requires cluster downtime or manual data export/import.

#### Proposed Solution
1. Design an online partition state machine: `STABLE` -> `MIGRATING` -> `SYNCHRONIZING` -> `STABLE`.
2. Implement chunked dual-writing and background key streaming from donor shards to recipient shards.
3. Update routing table dynamically with atomic epoch versioning to avoid routing to obsolete shard topologies.
4. Support rollbacks if a recipient node crashes during migration.

#### Acceptance Criteria
- [ ] Adding a new shard triggers automated data rebalancing in the background.
- [ ] Client reads and writes continue uninterrupted during partition transfer.
- [ ] In-flight transactions routing to keys in transit complete deterministically.

---

### Issue 4: Dynamic Cluster Membership Changes via Joint Consensus (Raft §6)

**Labels:** `raft`, `consensus`, `high-availability`

#### Problem Statement
Currently, cluster membership (`ClusterConfiguration`) is static. Adding or removing consensus replicas at runtime without stopping nodes requires implementing Raft Joint Consensus ($C_{\text{old,new}}$) to prevent dual-majority split-brain during configuration transitions.

#### Proposed Solution
1. Implement configuration change log entry types: `ConfigurationEntry(oldMembers, newMembers)`.
2. Implement joint consensus transition:
   - Leader proposes $C_{\text{old,new}}$; committed when majority of $C_{\text{old}}$ AND majority of $C_{\text{new}}$ acknowledge.
   - Leader proposes final $C_{\text{new}}$; committed when majority of $C_{\text{new}}$ acknowledges.
3. Handle node addition (non-voting learner catch-up before promotion) and leader step-down if removed from configuration.

#### Acceptance Criteria
- [ ] Dynamic addition and removal of nodes without cluster downtime or restarts.
- [ ] Automated tests verify at most one leader can be elected during configuration transition.
- [ ] Non-voting learner nodes successfully receive log snapshots before becoming voting members.

---

### Issue 5: Performance Profiling and Tail Latency (p99) Optimization Under High Contention

**Labels:** `performance`, `benchmarking`, `optimization`

#### Problem Statement
Under high thread contention and high write concurrency, lock contention in `TransactionRegistry` and disk flush latency in `DurableRaftLog` can increase 99th percentile (p99) latency.

#### Proposed Solution
1. Profile lock contention using async-profiler and JFR (Java Flight Recorder).
2. Replace coarse-grained synchronization in `MvccStore` and `StorageIndex` with lock-free data structures (e.g., `ConcurrentSkipListMap`, striped locks).
3. Implement group commit batching for `FsyncPolicy.ALWAYS`: coalesce multiple concurrent commit requests into a single `FileChannel.force(true)` call.
4. Benchmark latency distribution under varying batch sizes and concurrency levels.

#### Acceptance Criteria
- [ ] Group commit reduces p99 commit latency by $\ge 40\%$ under 64 concurrent writers.
- [ ] Benchmark harness produces verifiable p50, p90, p99, and p99.9 latency curves.
- [ ] Zero regression in durability or correctness invariants.

---

### Issue 6: Asynchronous Replication Pipelines & Pipelined AppendEntries

**Labels:** `raft`, `network`, `throughput`

#### Problem Statement
The leader currently replicates log entries synchronously per follower round-trip. While safe, this limits throughput over higher-latency networks (e.g., cross-datacenter links) where the network bandwidth-delay product is underutilized.

#### Proposed Solution
1. Implement sliding window replication pipelines: allow leader to transmit subsequent `AppendEntries` RPCs without waiting for the preceding response.
2. Track in-flight replication windows per follower with flow control to prevent overwhelming slow peers.
3. Automatically fall back to sequential replication upon detecting log rejection or network disconnects.

#### Acceptance Criteria
- [ ] Leader can maintain up to $K$ in-flight unacknowledged append batches per follower.
- [ ] Replicated throughput increases significantly over simulated high-latency links ($\ge 20\text{ms}$ RTT).
- [ ] Strict prefix consistency and commit index safety are maintained under all pipeline failure scenarios.
