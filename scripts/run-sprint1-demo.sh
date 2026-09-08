#!/usr/bin/env bash
set -e

echo "=============================================="
echo "  AegisDB: Run Sprint 1 Live Demonstration"
echo "=============================================="

mvn test-compile exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint1Demo \
    -Dexec.classpathScope=test
