#!/usr/bin/env bash
set -e

echo "======================================================================="
echo "  AegisDB: Run Sprint 5 Live Demonstration & Milestone M2 Gate"
echo "  Snapshots & Replicated Key-Value Store (US009 & US010)"
echo "======================================================================="

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint5Demo \
    -Dexec.classpathScope=test
