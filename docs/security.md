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

---

## 4. Configuration Precedence & Secrets Handling

To prevent accidental exposure of sensitive credentials in process listings (`ps aux`), AegisDB enforces a strict configuration precedence hierarchy:

```text
CLI Arguments > Environment Variables > YAML Configuration > Safe Defaults
```

### Secrets Rules
1. **No Tokens via CLI**: Passwords and bearer tokens (`AEGISDB_ADMIN_TOKEN`, `AEGISDB_MONITOR_TOKEN`) MUST NOT be passed directly as CLI flags where they could appear in system process tables or shell histories.
2. **Environment Variable Substitution**: The YAML configuration parser automatically resolves `${VAR_NAME}` placeholders from environment variables at runtime.
3. **Fail-Closed Validation**: If TLS is enabled (`security.tls.enabled: true`), certificate paths (`cert_path`, `key_path`) must be non-empty, existing files; otherwise the node fails fast and terminates at startup.

---

## 5. Management API & Transport Security

### Local Development Mode
- Transport security runs in explicit dev/test plaintext mode when TLS is not configured.
- Embedded management server binds to loopback (`127.0.0.1`) by default to prevent accidental external network exposure.

### Production Requirements
- **Inter-Node gRPC**: In distributed production topologies spanning non-loopback networks, TLS/mTLS MUST be enabled.
- **Management Plane HTTP**: The embedded management HTTP server should either remain bound to loopback or terminate behind an authenticating TLS reverse proxy (e.g., NGINX, Envoy, Caddy) providing TLS encryption, rate limiting, and access logging.

