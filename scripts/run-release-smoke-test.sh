#!/usr/bin/env bash
set -e

mvn test -pl aegisdb-integration -Dtest=SystemReleaseIntegrationTest "$@"
