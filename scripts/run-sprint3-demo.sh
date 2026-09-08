#!/usr/bin/env bash
set -e

echo "=============================================="
echo "  AegisDB: Kör Sprint 3 Live Demonstration"
echo "  Raft Log Replication (US006)"
echo "=============================================="

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint3Demo \
    -Dexec.classpathScope=test
