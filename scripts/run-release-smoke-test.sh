#!/usr/bin/env bash
set -e

mvn test -pl aegisdb_integration -Dtest=MasterCapstoneIntegrationTest "$@"
