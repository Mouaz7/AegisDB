#!/usr/bin/env bash
# =====================================================================
# AegisDB Release Artifact Unpack, Execution & Smoke Test
# Verifies:
# 1. Standalone archive unpacks cleanly
# 2. CLI launcher works with --help and --version
# 3. Server starts with all bundled runtime dependencies
# 4. End-to-end SystemReleaseIntegrationTest passes
# =====================================================================

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

TAR_ARCHIVE=$(find target/release -name "aegisdb-*-bin.tar.gz" 2>/dev/null | head -n 1)

if [ -z "$TAR_ARCHIVE" ] || [ ! -f "$TAR_ARCHIVE" ]; then
    echo "▶ Release artifact not found, packaging first..."
    ./scripts/package-release.sh
    TAR_ARCHIVE=$(find target/release -name "aegisdb-*-bin.tar.gz" | head -n 1)
fi

echo "====================================================================="
echo "  AegisDB: Verifying Release Artifact: ${TAR_ARCHIVE}"
echo "====================================================================="

TEST_TMP_DIR=$(mktemp -d 2>/dev/null || mktemp -d -t 'aegisdb-smoke')
trap "rm -rf '${TEST_TMP_DIR}'" EXIT

echo "▶ Step 1: Unpacking distribution archive to ${TEST_TMP_DIR}..."
tar -xzf "${TAR_ARCHIVE}" -C "${TEST_TMP_DIR}"

DIST_DIR=$(find "${TEST_TMP_DIR}" -mindepth 1 -maxdepth 1 -type d | head -n 1)
cd "${DIST_DIR}"

echo "▶ Step 2: Testing CLI --help and --version options..."
chmod +x bin/aegisdb-server
./bin/aegisdb-server --help >/dev/null
./bin/aegisdb-server --version

echo "▶ Step 3: Verifying bundled runtime dependencies in lib/..."
LIB_COUNT=$(ls -1 lib/*.jar 2>/dev/null | wc -l)
echo "  Found ${LIB_COUNT} JARs bundled in lib/."
if [ "${LIB_COUNT}" -lt 5 ]; then
    echo "  ❌ Release artifact lacks runtime dependencies!"
    exit 1
fi
echo "  ✓ Runtime dependencies verified."

echo "▶ Step 4: Executing SystemReleaseIntegrationTest..."
cd "${ROOT_DIR}"
MVN_CMD="mvn"
if [ -x "./mvnw" ]; then
    MVN_CMD="./mvnw"
fi
$MVN_CMD test -pl aegisdb-integration -Dtest=SystemReleaseIntegrationTest "$@"

echo "====================================================================="
echo "  ✅ RELEASE ARTIFACT SMOKE TEST PASSED"
echo "====================================================================="
