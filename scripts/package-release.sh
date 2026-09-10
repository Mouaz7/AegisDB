#!/usr/bin/env bash
# =====================================================================
# AegisDB Production Release Packaging Script
# Generates aegisdb-1.0.0-bin.tar.gz, aegisdb-1.0.0-bin.zip, and SHA-256 checksums
# =====================================================================

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="1.0.0"
DIST_NAME="aegisdb-${VERSION}"
RELEASE_DIR="${ROOT_DIR}/target/release"
STAGING_DIR="${RELEASE_DIR}/${DIST_NAME}"

echo "====================================================================="
echo "  AegisDB: Packaging Production Release Distribution"
echo "  Version: ${VERSION}"
echo "====================================================================="

cd "${ROOT_DIR}"

echo "▶ Step 1: Compiling and packaging Maven modules..."
mvn clean package -DskipTests

echo "▶ Step 2: Preparing distribution layout in ${STAGING_DIR}..."
rm -rf "${RELEASE_DIR}"
mkdir -p "${STAGING_DIR}/bin"
mkdir -p "${STAGING_DIR}/lib"
mkdir -p "${STAGING_DIR}/config"
mkdir -p "${STAGING_DIR}/docs"
mkdir -p "${STAGING_DIR}/scripts"

# Copy binary launchers
cp "${ROOT_DIR}/bin/"* "${STAGING_DIR}/bin/" 2>/dev/null || true
chmod +x "${STAGING_DIR}/bin/"* 2>/dev/null || true

# Copy configs
cp -r "${ROOT_DIR}/config/"* "${STAGING_DIR}/config/" 2>/dev/null || true

# Copy documentation and metadata
cp "${ROOT_DIR}/README.md" "${STAGING_DIR}/"
cp "${ROOT_DIR}/LICENSE" "${STAGING_DIR}/"
cp -r "${ROOT_DIR}/docs/"* "${STAGING_DIR}/docs/"

# Copy scripts
cp "${ROOT_DIR}/scripts/"*.sh "${STAGING_DIR}/scripts/"
chmod +x "${STAGING_DIR}/scripts/"*.sh

# Collect all built module JARs
echo "▶ Step 3: Collecting module JARs..."
find . -maxdepth 2 -name "target" -type d | while read tdir; do
    find "$tdir" -maxdepth 1 -name "*.jar" ! -name "*-sources.jar" ! -name "*-javadoc.jar" -exec cp {} "${STAGING_DIR}/lib/" \;
done

# Step 4: Create archives
echo "▶ Step 4: Creating distributable archives..."
cd "${RELEASE_DIR}"
tar -czf "${DIST_NAME}-bin.tar.gz" "${DIST_NAME}"
zip -rq "${DIST_NAME}-bin.zip" "${DIST_NAME}"

# Step 5: Compute SHA-256 checksums
echo "▶ Step 5: Generating cryptographic SHA-256 checksums..."
sha256sum "${DIST_NAME}-bin.tar.gz" > "${DIST_NAME}-bin.tar.gz.sha256"
sha256sum "${DIST_NAME}-bin.zip" > "${DIST_NAME}-bin.zip.sha256"

echo "====================================================================="
echo "  AegisDB Release Packaging Complete!"
echo "  Artifacts in: ${RELEASE_DIR}"
echo "  - ${DIST_NAME}-bin.tar.gz ($(du -h "${DIST_NAME}-bin.tar.gz" | cut -f1))"
echo "  - ${DIST_NAME}-bin.zip    ($(du -h "${DIST_NAME}-bin.zip" | cut -f1))"
echo "  - SHA-256: $(cat "${DIST_NAME}-bin.tar.gz.sha256" | cut -d' ' -f1)"
echo "====================================================================="
