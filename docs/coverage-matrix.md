# RustFS Jepsen 24-Workload Coverage Matrix (5 Core + 19 Enterprise)

## Core Workloads (`--suite core`)

| Workload | Engine / Checker | Verified Invariants |
|---|---|---|
| `register` | Knossos `linearizable` (`S3Register`) | Linearizable PUT / GET / DELETE / conditional `If-Match` & `If-None-Match` CAS |
| `append` | Elle `list-append` | Strict serializability (G0, G1a, G1b, G1c, G-single, G2-item) |
| `versioned` | `versioned-checker` | Non-empty `VersionId`, zero lost acknowledged versions, SHA-256 integrity, `ListObjectVersions` completeness |
| `multipart` | `multipart-checker` | 5 MiB S3-compliant multipart all-or-nothing visibility & exact post-heal SHA-256 |
| `list` | `list-consistency-checker` | Complete `ListObjectsV2`, zero deleted resurrections, zero phantom keys |

## Enterprise Workloads (`--suite enterprise`)

| # | Workload | Checker | Key Distributed & Chaos Invariants |
|---:|---|---|---|
| 1 | `site-replication` | `site-replication-checker` | Active-active multi-site convergence across IAM/policies/buckets/objects; zero loops |
| 2 | `bucket-replication` | `bucket-replication-checker` | Cross-cluster version & delete-marker replication; out-of-order stale-gen rejection; `REPLICA` loop guard |
| 3 | `worm` | `worm-checker` | `COMPLIANCE` / `GOVERNANCE` / Legal Hold enforcement against bypass & clock-skew attacks |
| 4 | `lifecycle-transition` | `lifecycle-transition-checker` | Cold-tier transition never clobbers newer client overwrite or loses data on tier outage |
| 5 | `lifecycle-expiration` | `lifecycle-expiration-checker` | Accurate age expiration without premature deletion or WORM-locked expiry |
| 6 | `lifecycle-tag` | `lifecycle-tag-checker` | `Prefix + Tag` AND-rules & TOCTOU safety when objects are re-tagged mid-scan |
| 7 | `pool-expansion` | `pool-expansion-checker` | Zero-downtime pre/post-expansion reads and exact cross-pool `ListObjectsV2` union |
| 8 | `pool-decommission` | `pool-decommission-checker` | Write-admission fence (`Decommissioning` rejects new PUTs) and 100% source drain |
| 9 | `pool-rebalance` | `pool-rebalance-checker` | Cross-pool utilization spread reduction with zero missing/duplicate keys |
| 10 | `decommission-safety` | `decommission-safety-checker` | Concurrent overwrite & delete barriers, WORM retention preservation, crash durability |
| 11 | `rebalance-safety` | `rebalance-safety-checker` | Source-shard bitrot healing, destination-durable-before-source-delete, zero duplicate listings |
| 12 | `oidc` | `oidc-checker` | Fail-closed rejection of forged/expired/wrong-aud/revoked/retired-kid tokens; JWKS rotation & outage caching |
| 13 | `notification` | `notification-checker` | At-least-once event delivery per prefix/suffix filter; S3 data path never blocked by target outage |
| 14 | `audit` | `audit-checker` | Zero missing audit entries, intact tamper-evident hash chain, zero credential/secret leaks |
| 15 | `tiering` | `tiering-checker` | In-use remote tier deletion rejected (`TierInUse`); credential rotation preserves stub readability |
| 16 | `tiered-restore` | `tiered-restore-checker` | Bit-exact `RestoreObject`, anti-rollback (`#3074`), and TTL cache expiry preserving cold stub |
| 17 | `s3-tables` | `s3-tables-checker` | Strictly single winner per Iceberg `versionToken` CAS, unbroken snapshot lineage, non-empty namespace protection |
| 18 | `quota` | `quota-checker` | `HARD` quota limit enforced across PUT/CopyObject/multipart; zero byte leaks; WORM-safe `FIFO` eviction |
| 19 | `on-demand-migration` | `on-demand-migration-checker` | Bit-exact lazy pull-through; local overwrite & delete tombstones strictly shadow legacy source (zero resurrection) |
