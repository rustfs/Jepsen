#!/usr/bin/env bash
set -euo pipefail

# Bring up one official RustFS Jepsen topology (4x4 or 3x2) cleanly, removing
# any previous topology volumes so format.json layouts are never mixed.

TOPO="${1:-4x4}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${TOPO}" != "4x4" && "${TOPO}" != "3x2" ]]; then
  echo "Usage: $0 <4x4|3x2>" >&2
  exit 1
fi

echo "==> Tearing down existing Jepsen Docker topologies..."
docker compose -p rustfs-jepsen-4x4 -f "${ROOT_DIR}/docker/docker-compose.4x4.yml" down -v --remove-orphans || true
docker compose -p rustfs-jepsen-3x2 -f "${ROOT_DIR}/docker/docker-compose.3x2.yml" down -v --remove-orphans || true

echo "==> Starting ${TOPO} topology..."
docker compose -p "rustfs-jepsen-${TOPO}" -f "${ROOT_DIR}/docker/docker-compose.${TOPO}.yml" up -d --build
echo "==> ${TOPO} topology is ready. Run tests via ./scripts/run-suite.sh ${TOPO}"
