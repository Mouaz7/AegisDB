# AegisDB Failure Model

---

## 1. Assumptions Handled by the System

AegisDB is designed for an asynchronous distributed environment with fault-prone nodes and unreliable networks (fail-stop / crash-recovery model):

1. **Nodes Can Crash (Crash):**
   Nodes may crash abruptly at any time without warning.
2. **Nodes Can Restart (Crash-Recovery):**
   Nodes that have crashed can restart, rejoin the cluster, recover persistent state from disk (WAL and snapshots), and synchronize missing entries from the leader.
3. **Messages May Be Delayed (Network Delay):**
   The network does not guarantee bounded latency. Messages may arrive arbitrarily late.
4. **Messages May Be Duplicated (Message Duplication):**
   Retries over the network can cause identical RPCs to be delivered more than once. All consensus and replication handlers are idempotent.
5. **Messages May Be Lost (Message Loss):**
   Packets may be dropped in transit. The transport layer relies on timeouts, retries, and heartbeat intervals.
6. **Network Partitions (Network Partitions):**
   The network may split into isolated partitions (e.g., minority vs. majority). Only the partition controlling a strict majority ($> N/2$) of nodes is permitted to elect a leader, accept writes, and commit state transitions.

---

## 2. Out-of-Scope Assumptions (What the System Does NOT Assume)

- **No Byzantine Faults:**
   Nodes are assumed not to behave maliciously, forge cryptographic signatures, or send arbitrary corrupt data intentionally.
- **No Malicious Cluster Members:**
   All nodes in the cluster are assumed to execute valid, uncorrupted AegisDB code.
