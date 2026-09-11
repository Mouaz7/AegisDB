#!/usr/bin/env bash
set -e

echo "======================================================================="
echo "  AegisDB: Run Comprehensive Stress & Performance Benchmark Suite"
echo "  Evaluating Modules: Phases 1 to 6 Under Concurrency & Load"
echo "======================================================================="

mvn exec:java \
    -pl aegisdb-integration \
    -Dexec.mainClass=se.mouaz.aegisdb.integration.StressBenchmarkSuite \
    -Dexec.classpathScope=test
