#!/usr/bin/env bash
set -e

echo "=============================================="
echo "  AegisDB: Run all unit and integration tests"
echo "=============================================="

mvn clean test
