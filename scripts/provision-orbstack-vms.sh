#!/usr/bin/env bash
set -euo pipefail

# Provision lightweight 1 CPU / 1 GiB RAM OrbStack Debian Bookworm VMs on macOS,
# mounting real 5 GiB ext4 loop-backed block devices (/var/lib/rustfs/data1..D)
# with distinct st_dev device IDs for multi-pool RustFS Jepsen verification.
#
# Usage:
#   ./scripts/provision-orbstack-vms.sh <3x2|4x4> [pools=2] [ssh_pubkey_file=~/.ssh/id_ed25519.pub]

TOPO="${1:-3x2}"
POOLS="${2:-2}"
PUBKEY_FILE="${3:-$HOME/.ssh/id_ed25519.pub}"
PREFIX="${VM_PREFIX:-rfs-n}"

if [[ "${TOPO}" == "3x2" ]]; then
  NODES_PER_POOL=3
  DRIVES_PER_NODE=2
elif [[ "${TOPO}" == "4x4" ]]; then
  NODES_PER_POOL=4
  DRIVES_PER_NODE=4
else
  echo "Usage: $0 <3x2|4x4> [pools] [ssh_pubkey_file]" >&2
  exit 1
fi

TOTAL_NODES=$(( NODES_PER_POOL * POOLS ))

if [[ ! -f "${PUBKEY_FILE}" ]]; then
  echo "Missing SSH public key at ${PUBKEY_FILE}" >&2
  exit 1
fi
PUBKEY_CONTENT="$(cat "${PUBKEY_FILE}")"

echo "==> Deleting existing OrbStack VMs to guarantee a clean 1C/1G slate..."
for vm in $(orbctl list -q 2>/dev/null || true); do
  orbctl delete -f "${vm}"
done

echo "==> Creating ${TOTAL_NODES} OrbStack Debian Bookworm VMs (1 CPU, 1 GiB RAM each)..."
for i in $(seq 1 "${TOTAL_NODES}"); do
  vm="${PREFIX}${i}"
  orbctl create --cpus 1 --memory 1G --disk 32G debian:bookworm "${vm}"
done

HOSTS_BLOCK=""
for i in $(seq 1 "${TOTAL_NODES}"); do
  vm="${PREFIX}${i}"
  ip="$(orbctl list | awk -v m="${vm}" '$1 == m {print $NF}')"
  if [[ -z "${ip}" ]]; then
    echo "Failed to resolve IP for ${vm}" >&2
    exit 1
  fi
  HOSTS_BLOCK+="${ip} ${vm} ${vm}.orb.local"$'\n'
done

echo "==> Configuring SSH, packages, /etc/hosts, and ${DRIVES_PER_NODE}x5GiB loop disks per VM..."
for i in $(seq 1 "${TOTAL_NODES}"); do
  vm="${PREFIX}${i}"
  orbctl run -m "${vm}" -u root bash -s -- "${DRIVES_PER_NODE}" "${PUBKEY_CONTENT}" "${HOSTS_BLOCK}" <<'EOS'
set -euo pipefail
DRIVES="$1"
PUBKEY="$2"
HOSTS="$3"

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq --no-install-recommends \
  ca-certificates curl iproute2 iptables iputils-ping openssh-server procps psmisc sudo unzip wget e2fsprogs >/dev/null

id -u jepsen >/dev/null 2>&1 || useradd -m -s /bin/bash jepsen
echo "jepsen ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/jepsen
chmod 0440 /etc/sudoers.d/jepsen
sed -i 's/^#*PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config
sed -i 's/^#*PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config

for home_dir in /root /home/jepsen; do
  mkdir -p "${home_dir}/.ssh"
  printf '%s\n' "${PUBKEY}" > "${home_dir}/.ssh/authorized_keys"
  chmod 700 "${home_dir}/.ssh"
  chmod 600 "${home_dir}/.ssh/authorized_keys"
done
chown -R jepsen:jepsen /home/jepsen/.ssh
mkdir -p /run/sshd
systemctl enable --now ssh >/dev/null 2>&1 || /usr/sbin/sshd

grep -v "rfs-n" /etc/hosts > /tmp/hosts.clean || true
printf '%s\n' "${HOSTS}" >> /tmp/hosts.clean
cp /tmp/hosts.clean /etc/hosts

mkdir -p /var/lib/rustfs-loop /var/lib/rustfs /var/log/rustfs
ulimit -n 65536 || true
cat <<'EOF' > /etc/security/limits.d/99-rustfs.conf
* soft nofile 65536
* hard nofile 65536
root soft nofile 65536
root hard nofile 65536
EOF

for d in $(seq 1 "${DRIVES}"); do
  mnt="/var/lib/rustfs/data${d}"
  img="/var/lib/rustfs-loop/disk${d}.img"
  if mountpoint -q "${mnt}"; then
    umount -f "${mnt}"
  fi
  rm -f "${img}"
  truncate -s 5G "${img}"
  mkfs.ext4 -q -F "${img}"
  mkdir -p "${mnt}"
  mount -o loop,noatime "${img}" "${mnt}"
done
EOS
done

echo "==> Cluster VMs ready (${TOPO}, ${POOLS} pools, ${TOTAL_NODES} VMs x 1C/1G, ${DRIVES_PER_NODE}x5GiB disks/VM):"
printf '%s' "${HOSTS_BLOCK}"
