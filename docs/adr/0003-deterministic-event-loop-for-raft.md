# ADR 0003: Deterministic Single-Threaded Event Loop for Raft Consensus

## Status
Accepted

## Context
Raft consensus algorithm safety depends critically on sequential, race-free state transitions (term advancement, vote granting, log append, and commit index management). Multi-threaded mutation of consensus state protected by arbitrary reentrant locks frequently leads to race conditions, priority inversions, and non-deterministic deadlocks (Master Plan §5, §7, §27).

Key requirements:
1. "Raft state mutation is serialized per Raft group; arbitrary concurrent threads must not mutate the consensus state." (Master Plan §5).
2. Elimination of concurrency race conditions in leader elections and log replication.
3. Deterministic execution for reproducible debugging and unit testing.

## Decision
We implement a **Single-Threaded Event Loop per Raft Node (`RaftNode`)**:
1. All state-mutating events are modeled as strongly-typed events:
   - `ElectionTimeoutEvent`
   - `HeartbeatTimeoutEvent`
   - `VoteRequestEvent`
   - `VoteResponseEvent`
   - `AppendEntriesEvent`
   - `AppendEntriesResponseEvent`
   - `ClientCommandEvent`
2. An event queue backed by a dedicated single-thread executor processes events sequentially in order.
3. Timers (`ElectionTimer`, `HeartbeatManager`) submit events into the queue rather than mutating state directly.
4. Concurrency test suites run against deterministic clocks and schedulers (`TestClock`, `DeterministicScheduler`).

## Consequences
### Positive
- Zero race conditions in Raft state transitions.
- Internal consensus state does not require complex locking or synchronizations.
- Invariant assertions can be checked at the boundary of every event processing step.

### Negative
- Handlers inside the event loop must remain strictly non-blocking; long-running operations (like disk flush or network dispatch) must be offloaded or executed asynchronously.
