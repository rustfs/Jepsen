#!/usr/bin/env bash
set -euo pipefail

# Install authorized SSH public key for root and jepsen users.
if [[ -n "${JEPSEN_SSH_PUBKEY:-}" ]]; then
  for user_home in /root /home/jepsen; do
    mkdir -p "${user_home}/.ssh"
    printf '%s\n' "${JEPSEN_SSH_PUBKEY}" > "${user_home}/.ssh/authorized_keys"
    chmod 700 "${user_home}/.ssh"
    chmod 600 "${user_home}/.ssh/authorized_keys"
  done
  chown -R jepsen:jepsen /home/jepsen/.ssh
fi

# Ensure the container's own hostname resolves to its primary bridge IP so
# RustFS correctly recognizes local endpoints in RUSTFS_VOLUMES.
NODE_HOST="${JEPSEN_NODE_HOSTNAME:-$(hostname)}"
PRIMARY_IP="$(hostname -i | awk '{print $1}')"
if [[ -n "${PRIMARY_IP}" ]] && ! grep -qE "^${PRIMARY_IP}[[:space:]]+${NODE_HOST}(\$|[[:space:]])" /etc/hosts; then
  printf '%s %s\n' "${PRIMARY_IP}" "${NODE_HOST}" >> /etc/hosts
fi

# Create data directories expected by the selected topology.
DRIVES="${RUSTFS_DRIVES_PER_NODE:-4}"
for i in $(seq 1 "${DRIVES}"); do
  mkdir -p "/var/lib/rustfs/data${i}"
done

exec /usr/sbin/sshd -D -e
