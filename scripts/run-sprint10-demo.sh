#!/usr/bin/env bash
set -e

echo "======================================================================="
echo "  AegisDB: Run Sprint 10 Live Demonstration"
echo "  Chaos Engineering, Fault Injection & Security Hardening (US016, US017)"
echo "======================================================================="

mvn exec:java \
    -pl aegisdb_integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.Sprint10Demo \
    -Dexec.classpathScope=test
