#!/usr/bin/env bash
set -e

echo "======================================================================="
echo "  AegisDB: Run Sprint 9 Live Demonstration"
echo "  Cross-Shard Distributed Transactions & Two-Phase Commit (2PC)"
echo "  Milestone M4 Gate Verification (US015)"
echo "======================================================================="

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint9Demo \
    -Dexec.classpathScope=test
