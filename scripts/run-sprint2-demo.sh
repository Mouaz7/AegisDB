#!/usr/bin/env bash
set -e

echo "=============================================="
echo "  AegisDB: Run Sprint 2 Live Demonstration"
echo "  Raft Leader Election (US005)"
echo "=============================================="

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint2Demo \
    -Dexec.classpathScope=test
