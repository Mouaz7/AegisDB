#!/usr/bin/env bash
set -e

echo "======================================================================="
echo "  AegisDB: Run Sprint 6 Live Demonstration"
echo "  Multi-Version Concurrency Control (MVCC) & Snapshot Isolation (US011)"
echo "======================================================================="

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint6Demo \
    -Dexec.classpathScope=test
