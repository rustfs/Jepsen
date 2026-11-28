#!/usr/bin/env bash
set -euo pipefail

# Execute a Jepsen workload suite (core, enterprise, scanner, or all) against a running topology.

TOPO="${1:-4x4}"
SUITE="${2:-core}"
TIME_LIMIT="${JEPSEN_TIME_LIMIT:-90}"
RATE="${JEPSEN_RATE:-10}"
FAULTS="${JEPSEN_FAULTS:-partition,kill,pause,packet,drive}"
SSH_USER="${JEPSEN_SSH_USER:-jepsen}"
SSH_KEY="${JEPSEN_SSH_KEY:-$HOME/.ssh/id_ed25519}"

if [[ "${TOPO}" == "4x4" || "${TOPO}" == "4x2" ]]; then
  CONCURRENCY="${JEPSEN_CONCURRENCY:-8}"
elif [[ "${TOPO}" == "3x2" ]]; then
  CONCURRENCY="${JEPSEN_CONCURRENCY:-6}"
else
  echo "Usage: $0 <4x4|3x2|4x2> [core|enterprise|scanner|all]" >&2
  exit 1
fi

echo "==> Running Jepsen test-all suite=${SUITE} topology=${TOPO} faults=${FAULTS}"
lein run test-all \
  --topology "${TOPO}" \
  --suite "${SUITE}" \
  --time-limit "${TIME_LIMIT}" \
  --concurrency "${CONCURRENCY}" \
  --rate "${RATE}" \
  --faults "${FAULTS}" \
  --username "${SSH_USER}" \
  --ssh-private-key "${SSH_KEY}"
