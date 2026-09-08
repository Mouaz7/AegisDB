#!/usr/bin/env bash
set -e

echo "=============================================="
echo "  AegisDB: Kör Sprint 4 Live Demonstration"
echo "  Persistence and Recovery (US007 & US008)"
echo "=============================================="

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint4Demo \
    -Dexec.classpathScope=test
