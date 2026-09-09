#!/usr/bin/env bash
set -e

echo "======================================================================="
echo "  AegisDB: Run Sprint 8 Live Demonstration"
echo "  Sharding and Routing across Multi-Raft Groups (US013, US014)"
echo "======================================================================="

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint8Demo \
    -Dexec.classpathScope=test
