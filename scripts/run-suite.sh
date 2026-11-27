#!/usr/bin/env bash
set -euo pipefail

# Execute all 5 Jepsen workloads sequentially against a running topology.

TOPO="${1:-4x4}"
TIME_LIMIT="${JEPSEN_TIME_LIMIT:-90}"
RATE="${JEPSEN_RATE:-10}"
FAULTS="${JEPSEN_FAULTS:-partition,kill,pause,packet,drive}"
SSH_USER="${JEPSEN_SSH_USER:-jepsen}"
SSH_KEY="${JEPSEN_SSH_KEY:-$HOME/.ssh/id_ed25519}"

if [[ "${TOPO}" == "4x4" ]]; then
  CONCURRENCY="${JEPSEN_CONCURRENCY:-8}"
elif [[ "${TOPO}" == "3x2" ]]; then
  CONCURRENCY="${JEPSEN_CONCURRENCY:-6}"
else
  echo "Usage: $0 <4x4|3x2>" >&2
  exit 1
fi

for workload in register append versioned multipart list; do
  echo "==> Running Jepsen workload=${workload} topology=${TOPO} faults=${FAULTS}"
  lein run test \
    --topology "${TOPO}" \
    --workload "${workload}" \
    --time-limit "${TIME_LIMIT}" \
    --concurrency "${CONCURRENCY}" \
    --rate "${RATE}" \
    --faults "${FAULTS}" \
    --username "${SSH_USER}" \
    --ssh-private-key "${SSH_KEY}"
done
