#!/usr/bin/env bash
set -euo pipefail

# Preflight verification for RustFS Jepsen target nodes.
# Checks SSH access, passwordless sudo, data directories, iptables, and tc netem.

SSH_USER="${JEPSEN_SSH_USER:-jepsen}"
SSH_KEY="${JEPSEN_SSH_KEY:-$HOME/.ssh/id_ed25519}"
DRIVES=4

while [[ $# -gt 0 ]]; do
  case "$1" in
    --drives) DRIVES="$2"; shift 2 ;;
    --user) SSH_USER="$2"; shift 2 ;;
    --key) SSH_KEY="$2"; shift 2 ;;
    *) break ;;
  esac
done

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 [--drives N] [--user USER] [--key PATH] host1 host2 ..." >&2
  exit 1
fi

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=5 -i "${SSH_KEY}")

for host in "$@"; do
  echo "==> Checking ${host}"
  ssh "${SSH_OPTS[@]}" "${SSH_USER}@${host}" "bash -s" <<EOF
set -euo pipefail
sudo -n true
echo "  sudo: ok"
sudo iptables -L >/dev/null
echo "  iptables: ok"
for i in \$(seq 1 ${DRIVES}); do
  test -d "/var/lib/rustfs/data\${i}"
done
echo "  drives: ${DRIVES} directories present"
if sudo tc qdisc show >/dev/null 2>&1; then
  echo "  tc netem: available"
else
  echo "  tc netem: NOT available (omit 'packet' from --faults)"
fi
EOF
done
echo "Preflight complete."
