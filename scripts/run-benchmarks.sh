#!/usr/bin/env bash
# =====================================================================
# AegisDB Reproducible Cluster Scale Benchmark Runner (3, 5, 7 nodes)
# =====================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "================================================================================"
echo "   AEGISDB - MULTI-NODE SCALE & PROFILING BENCHMARK HARNESS                     "
echo "   Evaluating: 3, 5, and 7 Nodes across Read-Heavy, Write-Heavy, and Contended  "
echo "================================================================================"

cd "${ROOT_DIR}"

mkdir -p experiments/results

# Run ClusterScaleBenchmark via exec-maven-plugin or java with classpath
./mvnw test-compile -pl aegisdb-benchmark -DskipTests -q

CP=$(./mvnw dependency:build-classpath -pl aegisdb-benchmark | grep -v '\[INFO\]' | tail -n 1)
BENCH_CLASSES="${ROOT_DIR}/aegisdb-benchmark/target/classes:${ROOT_DIR}/aegisdb-benchmark/target/test-classes"

java -cp "${BENCH_CLASSES}:${CP}" se.mouaz.aegisdb.benchmark.ClusterScaleBenchmark

echo "================================================================================"
echo "   BENCHMARK COMPLETED SUCCESSFULLY                                             "
echo "   Markdown report: experiments/results/scale_benchmarks.md                     "
echo "   CSV data:        experiments/results/scale_benchmarks.csv                    "
echo "================================================================================"
