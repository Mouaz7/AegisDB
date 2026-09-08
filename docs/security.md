# AegisDB Secure-by-Design Plan & Security Controls

## 1. Threat Model
AegisDB operates on a crash-recovery model under asynchronous network assumptions (Master Project Plan §12). Byzantine cluster members are explicitly out of scope. Security engineering protects the management plane, cluster transport, input boundaries, credentials, dependencies, local storage files, logs, and the CI/CD pipeline.

---

## 2. Security Controls by Area

| Area | Security Controls Implemented / Required |
| :--- | :--- |
| **Input Validation** | Strictly bound key sizes (max 1024 bytes), value sizes (max 16 MB), message sizes, batch sizes, and numeric ranges. Reject malformed or oversized payloads at network boundaries. |
| **Resource Exhaustion** | Bounded queues, bounded concurrency, backpressure on writes, bounded retry attempts, bounded transaction lifetimes, and chunked snapshot transfers (64 KB). |
| **Cluster Transport** | Peer identity validation, connection timeouts, TLS/mTLS encryption for inter-node communication across insecure networks. |
| **Data at Rest** | Restrictive file system permissions (`0600` for files, `0700` for directories). Path traversal protections prevent arbitrary file writes. Encryption-at-rest documented for production storage. |
| **Serialization Safety** | Protocol Buffers exclusively for wire RPCs; reject unknown or corrupted fields; eliminate unsafe Java native deserialization across all public boundaries. |
| **Secrets Hygiene** | No hardcoded secrets, passwords, or tokens in git repositories, YAML configuration defaults, logs, or exception messages. |
| **Dependency Security** | Managed, pinned dependency versions via parent BOM. Automated vulnerability scanning (SpotBugs, PMD, OWASP dependency checks) in CI/CD. |
| **Logging Sanitization** | Sensitive keys and payload contents are masked or truncated in debug/info logs; stack traces never leak sensitive internal storage paths. |
| **CI/CD Pipeline** | Least-privilege GitHub Actions tokens, protected `main` branch, required status checks on all pull requests, and signed release tags. |

---

## 3. Automated Security Verification
- **Path Traversal Test**: Validates that crafted directory names or filenames cannot escape the configured data directory.
- **Malformed Protocol Buffer Tests**: Corrupted bytes, negative lengths, and invalid magic numbers immediately trigger `CorruptedWalException` or `TransportException`.
- **Oversized Record Rejection**: Attempts to persist or transmit records exceeding the 16 MB boundary are safely rejected before memory allocation.
