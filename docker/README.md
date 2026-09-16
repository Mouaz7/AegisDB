# Docker Deployment & Observability Stack

This directory contains container configurations for running AegisDB nodes along with Prometheus and Grafana.

## 1. Building the AegisDB Container Image

From the repository root:
```bash
docker build -t aegisdb:0.1.0-alpha.1 .
```

## 2. Running Prometheus & Grafana

Start the observability infrastructure:
```bash
cd docker
docker compose up -d
```

- **Prometheus UI**: [http://localhost:9090](http://localhost:9090)
  - Pre-configured to scrape AegisDB management endpoints on `9001`, `9002`, and `9003`.
- **Grafana Dashboards**: [http://localhost:3000](http://localhost:3000)
  - Default credentials: `admin` / `aegisdb`
  - Pre-loaded dashboard: **AegisDB Cluster Telemetry & Observability** (throughput, P50/P95/P99 latency, Raft term, log size, MVCC abort rate).

## 3. Stopping the Stack
```bash
docker compose down -v
```
