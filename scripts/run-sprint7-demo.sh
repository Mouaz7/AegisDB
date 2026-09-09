#!/usr/bin/env bash
set -e

echo "======================================================================="
echo "  AegisDB: Run Sprint 7 Live Demonstration"
echo "  Single-Shard Transactions & Concurrency Control (US012, Milestone M3)"
echo "======================================================================="

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint7Demo \
    -Dexec.classpathScope=test
