#!/usr/bin/env bash
set -e

echo "======================================================================="
echo "  AegisDB: Run Sprint 12 Master Capstone Demonstration"
echo "  Complete 16-Step End-to-End Operational Verification Scenario (§26)"
echo "  Master Project Plan §1, §3, §4, §10, §12, §15, §17, §20, §26 & §28"
echo "======================================================================="

ARGS="$@"

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint12Demo \
    -Dexec.classpathScope=test \
    -Dexec.args="$ARGS"
