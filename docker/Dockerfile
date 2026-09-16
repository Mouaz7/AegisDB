# =====================================================================
# Multi-stage Dockerfile for AegisDB Distributed Database
# =====================================================================

# Stage 1: Build stage
FROM eclipse-temurin:21-jdk-jammy AS builder

WORKDIR /build

# Copy Maven Wrapper and POMs first for dependency caching
COPY .mvn .mvn
COPY mvnw mvnw.cmd pom.xml ./
COPY aegisdb-benchmark/pom.xml aegisdb-benchmark/
COPY aegisdb-chaos/pom.xml aegisdb-chaos/
COPY aegisdb-client/pom.xml aegisdb-client/
COPY aegisdb-common/pom.xml aegisdb-common/
COPY aegisdb-integration/pom.xml aegisdb-integration/
COPY aegisdb-management/pom.xml aegisdb-management/
COPY aegisdb-mvcc/pom.xml aegisdb-mvcc/
COPY aegisdb-node/pom.xml aegisdb-node/
COPY aegisdb-observability/pom.xml aegisdb-observability/
COPY aegisdb-protocol/pom.xml aegisdb-protocol/
COPY aegisdb-raft/pom.xml aegisdb-raft/
COPY aegisdb-sharding/pom.xml aegisdb-sharding/
COPY aegisdb-storage/pom.xml aegisdb-storage/
COPY aegisdb-transaction/pom.xml aegisdb-transaction/
COPY aegisdb-transport/pom.xml aegisdb-transport/

RUN ./mvnw dependency:go-offline -B || true

# Copy all source and packaging scripts
COPY . .

# Build and package distribution
RUN chmod +x mvnw bin/* scripts/*.sh && \
    ./scripts/package-release.sh

# Stage 2: Runtime image
FROM eclipse-temurin:21-jre-jammy

LABEL maintainer="Mouaz <https://github.com/Mouaz7>"
LABEL org.opencontainers.image.title="AegisDB"
LABEL org.opencontainers.image.description="High-performance, transactional, distributed partitioned database engine"
LABEL org.opencontainers.image.version="0.1.0-alpha.1"
LABEL org.opencontainers.image.licenses="MIT"

ENV AEGISDB_HOME=/opt/aegisdb
ENV PATH="${AEGISDB_HOME}/bin:${PATH}"

# Install curl for container health checks
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# Run as non-root user for security
RUN groupadd -r aegisdb && useradd -r -g aegisdb -d /var/lib/aegisdb aegisdb

WORKDIR ${AEGISDB_HOME}

# Copy packaged release from builder stage
COPY --from=builder /build/target/release/aegisdb-0.1.0-alpha.1 ${AEGISDB_HOME}

# Ensure storage directories exist and have proper permissions
RUN mkdir -p /var/lib/aegisdb/data && \
    chown -R aegisdb:aegisdb ${AEGISDB_HOME} /var/lib/aegisdb

USER aegisdb

# Client RPC (7001), Raft Peer RPC (8001), Management HTTP (9001)
EXPOSE 7001 8001 9001

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://127.0.0.1:9001/metrics || exit 1

ENTRYPOINT ["/opt/aegisdb/bin/aegisdb-server"]
CMD ["--config", "/opt/aegisdb/config/aegisdb-cluster.example.yaml", "--node-id", "node-1"]
