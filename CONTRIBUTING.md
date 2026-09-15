# Contributing to AegisDB

Thank you for your interest in contributing to AegisDB! We welcome contributions from the community to help improve this distributed transactional database engine.

---

## Code of Conduct

All contributors and maintainers are expected to follow the [Code of Conduct](CODE_OF_CONDUCT.md). Please report any unacceptable behavior to the project maintainers.

---

## Development Prerequisites

- **Java Development Kit (JDK)**: OpenJDK 21 LTS or newer.
- **Maven**: Included via `./mvnw` (Apache Maven 3.9+).
- **Git**: Configured with LF line endings.
- **Docker** (Optional): For running integration Grafana/Prometheus telemetry stacks.

---

## Development Workflow

1. **Fork and Clone the Repository**:
   ```bash
   git clone https://github.com/Mouaz7/AegisDB.git
   cd AegisDB
   ```

2. **Create a Feature Branch**:
   ```bash
   git checkout -b feature/my-new-feature
   ```

3. **Build the Project**:
   ```bash
   ./mvnw clean test-compile
   ```

4. **Run the Test Suite**:
   Before submitting any changes, verify that all tests and architectural gates pass:
   ```bash
   ./mvnw test
   ./scripts/verify-release-readiness.sh
   ```

5. **Commit Conventions**:
   Follow [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat(...)`: A new feature
   - `fix(...)`: A bug fix
   - `docs(...)`: Documentation changes
   - `refactor(...)`: Code refactoring without behavior change
   - `test(...)`: Adding or correcting tests
   - `chore(...)`: Build process or tooling changes

6. **Submit a Pull Request**:
   - Push your branch to GitHub.
   - Open a PR against `main` (or `develop` when active).
   - Ensure the PR template is filled out with details on changes and verification.

---

## Architecture and Quality Guidelines

- **Decoupled Architecture**: Strictly honor the module boundaries. Core consensus (`aegisdb-raft`) and storage (`aegisdb-storage`) must not depend on higher layers like `aegisdb-management` or `aegisdb-client`.
- **ArchUnit Tests**: Architecture invariant rules are enforced in `aegisdb-integration`. Any violation of isolation layers will fail the build.
- **Secrets & Security**: Never commit credentials, passwords, or live tokens. Use placeholders (`${AEGISDB_ADMIN_TOKEN}`) in sample configurations.
- **Protobuf Protocol Stability**: Tag numbers in `raft_rpc.proto` must never be altered or reused once defined.
