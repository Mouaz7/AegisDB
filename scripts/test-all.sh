#!/usr/bin/env bash
set -e

echo "=============================================="
echo "  AegisDB: Kör alla enhets- och integrationstester"
echo "=============================================="

mvn clean test
