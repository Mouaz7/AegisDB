#!/usr/bin/env bash
set -e

echo "======================================================================="
echo "  AegisDB: Run Sprint 11 Live Demonstration"
echo "  Observability, Prometheus, Grafana, and Research Benchmarks (US018, US019)"
echo "======================================================================="

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint11Demo \
    -Dexec.classpathScope=test
