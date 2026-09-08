# AegisDB Engineering and Language Standards

## 1. Strict English Language Mandate
- **All code, tests, documentation, comments, commit messages, pull requests, issues, diagrams, and logs must be 100% in professional English.**
- Swedish terminology is strictly prohibited across the entire codebase.
  - Correct: `startElection()`, `currentTerm`, `leaderId`, `recoverFromWal()`
  - Prohibited: `startaVal()`, `aktuellTermin`, `ledareId`, `aterstallWal()`
- Agent responses to the user must always be formatted in clear, professional English.

## 2. Execution Environment (Windows & WSL)
- Do NOT chain bash commands using `&&` directly in PowerShell (e.g. `wsl cmd1 && wsl cmd2` will error).
- Always wrap chained or script commands using:
  `wsl bash -c "<command1> && <command2>"`
- Ensure scripts in `scripts/` have executable permissions (`chmod +x`).

## 3. Distributed Systems & Concurrency Invariants
- **No Sleep-Driven Tests**: Use `DeterministicScheduler`, `TestClock`, or `Awaitility`. Never introduce arbitrary `Thread.sleep`.
- **Single-Threaded Raft Event Loop**: State mutations per Raft group must be strictly serialized through the event loop; do not introduce multi-threaded mutations to consensus state.
- **Snapshot Isolation**: Enforce First-Committer-Wins conflict detection to prevent lost updates under concurrency.
- **Architecture Purity**: Core algorithmic modules (`common`, `raft`, `storage`, `mvcc`) must never depend on Spring, gRPC, or management frameworks.
