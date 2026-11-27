#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
docker compose -p rustfs-jepsen-4x4 -f "${ROOT_DIR}/docker/docker-compose.4x4.yml" down -v --remove-orphans || true
docker compose -p rustfs-jepsen-3x2 -f "${ROOT_DIR}/docker/docker-compose.3x2.yml" down -v --remove-orphans || true
echo "All RustFS Jepsen topologies stopped and volumes cleaned."
