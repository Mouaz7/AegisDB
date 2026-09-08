# ADR 0001: Java 25 LTS Baseline

## Status
Accepted

## Context
AegisDB is designed as a modern, high-performance distributed transactional database engine built from scratch. Choosing the runtime platform and language level is a foundational architectural decision that impacts concurrency primitives, performance, standard library capabilities, and project longevity (Master Plan §2, §16, §24).

Key considerations:
1. Long-Term Support (LTS) stability.
2. Modern language features: records, pattern matching, sealed classes, virtual threads, modern collections.
3. High-throughput memory management and standard library concurrency primitives (`AtomicReference`, `VarHandle`, `ConcurrentHashMap`).
4. Consistent multi-module Maven build and CI tooling.

## Decision
We adopt **Java 25 LTS** as the baseline compiler release and runtime standard for AegisDB across all Maven modules.

Specific guidelines:
- Parent POM enforces `<maven.compiler.release>25</maven.compiler.release>`.
- Prefer modern Java standard library concurrency and collection primitives in algorithmic core modules (`common`, `raft`, `storage`, `mvcc`).
- Prohibit framework magic (such as Spring annotations or reflections) in core algorithmic modules.

## Consequences
### Positive
- Access to modern Java language ergonomics (compact records, pattern matching, enhanced switch).
- Robust, standardized memory model and performance characteristics.
- Clean dependency profile without legacy polyfills.

### Negative
- Requires developer and CI environments to have JDK 25 installed.
