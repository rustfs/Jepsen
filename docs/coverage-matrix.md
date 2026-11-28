# RustFS Jepsen 31-Workload Coverage Matrix (5 Core + 19 Enterprise + 7 Scanner)

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

## Scanner-Centric Issue-Driven Workloads (`--suite scanner`, 7 Workloads)

Directly targeting the 7 real production scanner/heal/bitrot/MRF/ILM/replication/pool-fencing failure classes from 75+ `rustfs/rustfs` and `rustfs/backlog` issues:

| # | Workload | Checker | Upstream Issues & Verified Invariants |
|---:|---|---|---|
| S1 | `scanner-checkpoint-failover` | `scanner-checkpoint-failover-checker` | `#7108`, `#8494`, `#7964`, `#8461`, `#8457`, `#8618`, `#8141`, `#8297`, `#8888`: SATB convergence under writes, checkpoint survival across leader failover & lock timeouts, `scanner_cycle_mismatch` self-recovery, retryable follower recovery |
| S2 | `scanner-usage-convergence` | `scanner-usage-convergence-checker` | `#9520`, `#9471`, `#9664`, `#9672`, `#8478`, `#9543`, `#8670`, `#6958`, `#8106`, `#8924`: exits `usage_not_durable`/`Partial`, observes follower dirty marks, zero `ENOENT` debt, read-only `/tmp` restore, ILM usage decrement, zero cluster-wide quota 503 |
| S3 | `scanner-mrf-discharge` | `scanner-mrf-discharge-checker` | `#9629`, `#9370`, `#8192`, `#5950`, `#7951`, `#7997`, `#8590`, `#8608`, `#9671`, `#9154`: discharges healthy/legacy/absent MRF entries, prevents MRF Normal bursts from starving scanner/read-repair heals, converges stranded `xl.meta.bkp` |
| S4 | `scanner-deep-bitrot` | `scanner-deep-bitrot-checker` | `#5821`, `#5837`, `#8005`, `#8007`, `#5863`, `#8015`, `#8377`, `#8764`: in-place bitrot heal without `EEXIST`, `xl.meta` corruption & 1-byte tail truncation repair, stale delete-marker rejoin reconciliation, bounded Deep cadence |
| S5 | `scanner-delete-recovery` | `scanner-delete-recovery-checker` | `#9517`, `#9560`, `#8464`, `#8666`, `#9673`, `#9594`, `#9461`, `#9647`, `#9015`: zero phantom keys or resurrected buckets, outdated-gen heal, ignores `.rustfs`, bounded legacy-residue quarantine, `DeleteObject` succeeds under `ENOSPC`, cleans sub-quorum MPUs |
| S6 | `scanner-ilm-repl-coupling` | `scanner-ilm-repl-coupling-checker` | `#8620`, `#8454`, `#8768`, `#9354`, `#9670`, `#8834`, `#8758`, `#9434`, `#8321`, `#7959`: deleted ILM stays stopped after restart, benign already-deleted expiry, replication backfill & zero HEAD-405 heal loops, atomic restore copy-back, tierless no-op & corrupt skip |
| S7 | `scanner-pool-fence` | `scanner-pool-fence-checker` | `#8456`, `#8961`, `#8477`, `#9469`, `#7967`, `#8667`, `#7968`, `#9196`, `#8564`, `backlog#2932`, `#2147`: bounded cache working set, dynamic concurrency recovery, transient drive recovery, non-blocking status reads, >64 part probe, zero decommission `FileCorrupt` / cleanup orphans |
