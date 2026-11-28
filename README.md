# RustFS Jepsen — Distributed Consistency & Fault-Tolerance Suite

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**RustFS Jepsen** is the official [Jepsen](https://jepsen.io/) distributed verification suite for [RustFS](https://github.com/rustfs/rustfs). It deploys real multi-node RustFS erasure-coded clusters over SSH, drives concurrent S3 client traffic via the official AWS SDK v2, injects adversarial network, process, packet, drive, and clock faults, and verifies histories with **Knossos** and **Elle**.

## Workloads & Verified Properties

| Workload | Consistency Engine | Operations | Verified Properties |
|---|---|---|---|
| `register` | **Knossos** (`checker/linearizable`) | `PutObject`, `GetObject`, `DeleteObject`, conditional `If-Match` / `If-None-Match` CAS | Strict linearizability of single-object state transitions and atomic conditional writes |
| `append` | **Elle** (`elle.list-append`) | Conditional S3 read-modify-write list append | Strict serializability — catches G0, G1a (aborted read), G1b, G1c, G-single, and G2-item anomalies |
| `versioned` | Custom durability checker | S3 versioned `PutObject`, `GetObject(VersionId)`, `ListObjectVersions` | Non-empty `VersionId`, zero lost acknowledged versions, SHA-256 payload integrity, final listing completeness |
| `multipart` | Custom atomicity checker | `CreateMultipartUpload`, `UploadPart` (5 MiB non-final parts), `CompleteMultipartUpload`, `AbortMultipartUpload`, concurrent `GetObject` | All-or-nothing visibility (no partial part exposure) and exact post-heal SHA-256 integrity |
| `list` | Custom convergence checker | Concurrent `PutObject`, `DeleteObject`, post-heal `ListObjectsV2` | Every committed live key present, zero resurrected deleted keys, zero phantom keys |

## Nemesis Fault Matrix

Powered by `jepsen.nemesis.combined` plus a RustFS drive-fault nemesis:

- `partition`: Random halves, majorities ring, isolated single node, and primary partitions via `iptables`.
- `kill`: `SIGKILL` and restart of `rustfs` processes via `start-stop-daemon` (bypassing systemd `Restart=always`).
- `pause`: `SIGSTOP` / `SIGCONT` process stalls simulating long GC / kernel I/O freezes.
- `packet`: `tc netem` latency jitter (100ms ± 40ms) and 15% packet loss.
- `drive`: RustFS-specific single-drive failure (`chmod 0000 /var/lib/rustfs/dataN`) and recovery (`chmod 0755`) testing erasure-set degradation and healing.
- `clock`: Wall-clock skew bumps (enabled on independent Linux VMs / bare-metal nodes; omitted in shared-kernel Docker).

## Official Topologies

| Topology | Nodes × Drives | Total Drives | Parity | Read Quorum (`R`) | Write Quorum (`W`) | `RUSTFS_VOLUMES` Expansion |
|---|---:|---:|---:|---:|---:|---|
| `4x4` | 4 × 4 | 16 | 4 | 12 | 12 | `http://n{1...4}:9000/var/lib/rustfs/data{1...4}` |
| `3x2` | 3 × 2 | 6 | 3 | 3 | 4 | `http://n{1...3}:9000/var/lib/rustfs/data{1...2}` |
| `4x2` | 4 × 2 | 8 | 4 | 4 | 5 | `http://n{1...4}:9000/var/lib/rustfs/data{1...2}` |

## Quickstart

### 1. Run Checker & Topology Unit Tests (No Cluster Required)

Requires Java 17+ (Java 21 recommended) and [Leiningen](https://leiningen.org/):

```bash
lein test
lein run test --help
```

### 2. Run Against a Docker Cluster (`4x4` or `3x2`)

```bash
export JEPSEN_SSH_PUBKEY="$(cat ~/.ssh/id_ed25519.pub)"
export JEPSEN_SSH_KEY="$HOME/.ssh/id_ed25519"

# Start 4x4 topology (4 RustFS nodes + control container)
./scripts/up.sh 4x4

# Execute a single workload inside the control container
docker exec -it rustfs-jepsen-control \
  lein run test \
    --topology 4x4 \
    --workload register \
    --time-limit 90 \
    --concurrency 8 \
    --rate 10 \
    --faults partition,kill,pause,packet,drive \
    --username jepsen \
    --ssh-private-key /root/.ssh/id_ed25519

# Or run all 5 workloads sequentially
docker exec -it rustfs-jepsen-control ./scripts/run-suite.sh 4x4

# Tear down and switch to 3x2 (volumes are wiped so format.json is never reused)
./scripts/up.sh 3x2
```

### 3. Testing a Locally Built RustFS Binary

```bash
lein run test \
  --topology 4x4 \
  --workload append \
  --local-binary /path/to/linux-rustfs \
  --time-limit 120 \
  --faults partition,kill,pause,drive
```

### 4. Inspecting Results

Jepsen stores every run under `store/rustfs-<topology>-<workload>/latest/`:
- `results.edn` — top-level `:valid?` (`true` = passed, `false` = consistency violation, `:unknown` = inconclusive)
- `timeline.html` — interactive operation concurrency timeline
- `latency-raw.png` / `rate.png` — latency & throughput plots annotated with nemesis windows
- `n*/rustfs.log` — collected RustFS server logs from every cluster node

## Documentation

- [Complete 24-Workload Coverage Matrix (5 Core + 19 Enterprise Domains)](docs/coverage-matrix.md)
- [Topologies & Quorum Math](docs/topologies.md)
- [Fault Model, VM Clock Skew & Result Analysis](docs/fault-model.md)
- [Deterministic Simulation Testing Companion (`rustfs/DST`)](https://github.com/rustfs/DST)

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
