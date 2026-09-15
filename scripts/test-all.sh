#!/usr/bin/env bash
set -e

echo "=============================================="
echo "  AegisDB: Run all unit and integration tests"
echo "=============================================="

MVN_CMD="mvn"
if [ -x "./mvnw" ]; then
    MVN_CMD="./mvnw"
fi

$MVN_CMD clean test

