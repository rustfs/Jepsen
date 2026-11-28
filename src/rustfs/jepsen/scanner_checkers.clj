(ns rustfs.jepsen.scanner-checkers
  "Issue-driven Jepsen checkers for the 7 RustFS Scanner failure domains mapped
  directly to 75+ real `rustfs/rustfs` and `rustfs/backlog` issues:
    S1. scanner-checkpoint-failover-checker (#7108, #8494, #7964, #8461, #8457, #8618, #8141, #8297, #8888)
    S2. scanner-usage-convergence-checker   (#9520, #9471, #9664, #9672, #8305, #8478, #9543, #8670, #6958, #8106, #8924)
    S3. scanner-mrf-discharge-checker       (#9629, #9370, #8192, #5950, #7951, #7997, #8590, #8608, #9671)
    S4. scanner-deep-bitrot-checker         (#5821, #5837, #8005, #8007, #5863, #8015, #8377, #8764)
    S5. scanner-delete-recovery-checker     (#9517, #9560, #8464, #8666, #9673, #9594, #9461, #9647, #9015)
    S6. scanner-ilm-repl-coupling-checker   (#8620, #8454, #8768, #9354, #9670, #8834, #8758, #9434, #8321, #7959)
    S7. scanner-pool-fence-checker          (#8456, #8961, #8477, #9469, #7967, #8667, #7968, #9196, #8564)"
  (:require [jepsen.checker :as checker]))

(defn- ok-ops [history f]
  (filter #(and (= :ok (:type %)) (= f (:f %))) history))

;; ---------------------------------------------------------------------------
;; S1. Scanner Checkpoint Failover & SATB Convergence Checker
;; ---------------------------------------------------------------------------

(defn scanner-checkpoint-failover-checker
  "Verifies scanner cycle liveness and checkpoint survival under continuous writes,
  lock timeouts, object errors, leader failover, and planned node restarts:
    - Cycle MUST converge (not permanently Superseded under continuous writes, #7108/#8494).
    - Checkpoint cursor MUST survive leader failover without resetting to 0 (#7964/#8461).
    - Checkpoint saving MUST continue after a transient lock timeout or object error (#8457/#8618).
    - `scanner_cycle_mismatch` MUST self-recover without requiring leader rotation (#8141).
    - Surviving followers MUST stay retryable across planned restarts (#8297).
    - Scanner MUST resume after `cycle_max_directories` yields (#8888)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-checkpoint-verify))
            v (:value final-op)
            errors
            (cond-> []
              (nil? final-op)
              (conj {:type :missing-scanner-checkpoint-verify})

              (and v (not= :completed (:cycle-state v)))
              (conj {:type :scanner-cycle-did-not-converge :state (:cycle-state v)})

              (and v (not (:checkpoint-resumed-after-failover? v)))
              (conj {:type :checkpoint-lost-on-leader-failover :cursor (:resumed-cursor v)})

              (and v (not (:checkpoint-advanced-after-fault? v)))
              (conj {:type :checkpoint-stalled-after-timeout-or-error})

              (and v (pos? (or (:unresolved-cycle-mismatch v) 0)))
              (conj {:type :scanner-cycle-mismatch-deadlock :count (:unresolved-cycle-mismatch v)})

              (and v (:follower-permanently-paused? v))
              (conj {:type :follower-recovery-permanently-paused})

              (and v (false? (:resumed-after-max-dirs? v)))
              (conj {:type :scanner-halted-after-cycle-max-dirs}))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S2. Scanner Usage Census Durability & Dirty-Journal Convergence Checker
;; ---------------------------------------------------------------------------

(defn scanner-usage-convergence-checker
  "Verifies distributed bucket usage census convergence:
    - Final usage state MUST be `:certified` (never stuck in `usage_not_durable` #9520 or `partial` #9664).
    - `usage_baseline` MUST reflect follower writes (never stay 0 when objects exist, #9471).
    - Concurrent mid-walk `ENOENT` MUST leave `bucket_failure_debt == 0` (#8478/#9543).
    - Usage cache restore MUST succeed under read-only `/tmp` (#9672).
    - Corrupt legacy `.usage.json` MUST self-heal (#8670/#6951).
    - Post-ILM-expiration `objects_count` & `total_bytes` MUST decrement (#8106).
    - Quota queries for already-certified buckets MUST NOT return cluster-wide 503 during scan (#8924)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-usage-verify))
            v (:value final-op)
            errors
            (cond-> []
              (nil? final-op)
              (conj {:type :missing-scanner-usage-verify})

              (and v (not= :certified (:usage-state v)))
              (conj {:type :usage-not-durable-or-partial :state (:usage-state v)})

              (and v (pos? (or (:expected-objects v) 0))
                   (zero? (or (:observed-objects v) 0)))
              (conj {:type :usage-baseline-stuck-at-zero :expected (:expected-objects v)})

              (and v (pos? (or (:bucket-failure-debt v) 0)))
              (conj {:type :uncleared-bucket-failure-debt :debt (:bucket-failure-debt v)})

              (and v (false? (:readonly-tmp-restore-ok? v)))
              (conj {:type :readonly-tmp-usage-restore-failed})

              (and v (false? (:legacy-floor-self-healed? v)))
              (conj {:type :usage-floor-load-failed-deadlock})

              (and v (false? (:ilm-decrement-reflected? v)))
              (conj {:type :usage-did-not-decrement-after-ilm-expiry})

              (and v (pos? (or (:certified-bucket-quota-503s v) 0)))
              (conj {:type :cluster-wide-quota-503-during-scan :count (:certified-bucket-quota-503s v)}))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S3. Scanner MRF Discharge & Anti-Starvation Checker
;; ---------------------------------------------------------------------------

(defn scanner-mrf-discharge-checker
  "Verifies durable MRF responsibility discharge and heal lane fairness:
    - Healthy, MinIO-adopted legacy, and absent/deleted objects MUST discharge (`pending-mrf == 0`, #9629/#9370/#7997/#8590).
    - `recreate-failed-loops` MUST be 0 (absent objects classified absent, never retried forever).
    - `scanner-heal-executed` and `read-repair-executed` MUST both be > 0 under MRF Normal flood (#7951).
    - Stranded `xl.meta.bkp` rollback backups MUST converge without deleting valid child keys (#8608/#9671/#9154)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-mrf-verify))
            v (:value final-op)
            errors
            (cond-> []
              (nil? final-op)
              (conj {:type :missing-scanner-mrf-verify})

              (and v (pos? (or (:pending-mrf v) 0)))
              (conj {:type :undischarged-mrf-loop :pending (:pending-mrf v)})

              (and v (pos? (or (:recreate-failed-loops v) 0)))
              (conj {:type :absent-object-recreate-failed-loop :count (:recreate-failed-loops v)})

              (and v (zero? (or (:scanner-heal-executed v) 0)))
              (conj {:type :scanner-heal-starved-by-mrf-burst})

              (and v (zero? (or (:read-repair-executed v) 0)))
              (conj {:type :read-repair-starved-by-mrf-burst})

              (and v (false? (:stranded-backup-converged? v)))
              (conj {:type :stranded-rollback-backup-stalled})

              (and v (false? (:valid-child-preserved? v)))
              (conj {:type :parent-heal-deleted-valid-child}))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S4. Scanner Deep Bitrot, `xl.meta` Corruption & Tail-Truncation Checker
;; ---------------------------------------------------------------------------

(defn scanner-deep-bitrot-checker
  "Verifies Deep bitrot detection & in-place erasure heal invariants:
    - In-place bitrot heal MUST NOT fail with `EEXIST` (#5821).
    - `xl.meta` bitrot MUST be detected and healed (never skipped as Io ok, #5837).
    - 1-byte tail-truncated data shard MUST be detected and reconstructed (#8005).
    - Stale delete-marker metadata on rejoined drive MUST converge (#8007).
    - `drives-healed` count MUST equal actual repaired drives (#5863).
    - Deep-mode cadence MUST stay bounded (never stuck in Deep every cycle, #8015/#8377)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-bitrot-verify))
            v (:value final-op)
            errors
            (cond-> []
              (nil? final-op)
              (conj {:type :missing-scanner-bitrot-verify})

              (and v (pos? (or (:eexist-failures v) 0)))
              (conj {:type :inplace-heal-eexist-failure :count (:eexist-failures v)})

              (and v (false? (:xl-meta-bitrot-healed? v)))
              (conj {:type :xl-meta-bitrot-unhealed})

              (and v (false? (:tail-truncation-healed? v)))
              (conj {:type :one-byte-tail-truncation-missed})

              (and v (false? (:stale-delete-marker-reconciled? v)))
              (conj {:type :stale-delete-marker-survived-rejoin})

              (and v (false? (:drives-healed-accurate? v)))
              (conj {:type :inaccurate-drives-healed-metric})

              (and v (:stuck-in-permanent-deep-mode? v))
              (conj {:type :scanner-stuck-in-permanent-deep-mode}))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S5. Scanner Delete Recovery, Legacy Residue & ENOSPC Delete Checker
;; ---------------------------------------------------------------------------

(defn scanner-delete-recovery-checker
  "Verifies delete crash-consistency, recovery backoff, and full-disk deletion:
    - Zero phantom undeletable keys after crash mid-delete (#8464).
    - Zero deleted bucket resurrection after crash restart (#9647).
    - Outdated drive generation reconciled on rejoin (#9673).
    - `.rustfs` system dir ignored (`system-dir-deferred == 0`, #9594).
    - Ambiguous legacy residue retries bounded/quarantined (no infinite 1/s spin, #9517/#9560).
    - `DeleteObject` succeeds on 100%-full (`ENOSPC`) drive & reclaims space (#9461).
    - Sub-quorum stale multipart uploads cleaned up (#9015)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-delete-verify))
            v (:value final-op)
            errors
            (cond-> []
              (nil? final-op)
              (conj {:type :missing-scanner-delete-verify})

              (and v (pos? (or (:phantom-keys v) 0)))
              (conj {:type :phantom-undeletable-keys :count (:phantom-keys v)})

              (and v (pos? (or (:resurrected-buckets v) 0)))
              (conj {:type :deleted-bucket-resurrected :count (:resurrected-buckets v)})

              (and v (false? (:outdated-gen-reconciled? v)))
              (conj {:type :outdated-delete-generation-stuck})

              (and v (pos? (or (:system-dir-deferred v) 0)))
              (conj {:type :dot-rustfs-misclassified-as-bucket :count (:system-dir-deferred v)})

              (and v (:infinite-retry-storm? v))
              (conj {:type :delete-recovery-infinite-retry-storm})

              (and v (false? (:enospc-delete-succeeded? v)))
              (conj {:type :delete-object-503-on-enospc})

              (and v (false? (:sub-quorum-mp-cleaned? v)))
              (conj {:type :sub-quorum-multipart-leak}))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S6. Scanner ILM, Replication Backfill & Tier Recovery Coupling Checker
;; ---------------------------------------------------------------------------

(defn scanner-ilm-repl-coupling-checker
  "Verifies scanner-hosted ILM, replication backfill, and tier free-version coupling:
    - Deleted lifecycle rule MUST NOT continue expiring versions after restart (#8620).
    - Already-deleted objects during expiry sweep MUST NOT cause error/heap storms (#8454).
    - Existing-object replication backfill MUST finish across restarts (#8768) without inflating usage (#9354).
    - Live delete worker HEAD 405 MUST NOT loop scanner HEAL (#7959).
    - Restore copy-back MUST finalize metadata before scanner expiry can act (#8834).
    - Tier free-version recovery MUST be a no-op on tierless clusters (#8758), single-leader (#8321),
      and quarantine `FileCorrupt` orphans so tier removal is never blocked (#9434)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-ilm-repl-verify))
            v (:value final-op)
            errors
            (cond-> []
              (nil? final-op)
              (conj {:type :missing-scanner-ilm-repl-verify})

              (and v (:deleted-rule-still-expiring? v))
              (conj {:type :deleted-ilm-rule-resurrected-after-restart})

              (and v (pos? (or (:already-deleted-error-storm v) 0)))
              (conj {:type :ilm-already-deleted-error-storm :count (:already-deleted-error-storm v)})

              (and v (false? (:backfill-completed? v)))
              (conj {:type :replication-backfill-abandoned})

              (and v (:site-repl-size-inflated? v))
              (conj {:type :site-replication-continuous-size-growth})

              (and v (pos? (or (:head405-heal-retries v) 0)))
              (conj {:type :head405-delete-marker-heal-loop :retries (:head405-heal-retries v)})

              (and v (:premature-restore-expiry? v))
              (conj {:type :restore-copyback-premature-expiry})

              (and v (pos? (or (:tierless-bucket-walks v) 0)))
              (conj {:type :tier-recovery-walked-tierless-cluster :walks (:tierless-bucket-walks v)})

              (and v (false? (:tier-corrupt-orphan-skipped? v)))
              (conj {:type :tier-removal-wedged-by-file-corrupt}))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S7. Scanner Pacing, Bounded Memory, Drive Recovery & Pool Fence Checker
;; ---------------------------------------------------------------------------

(defn scanner-pool-fence-checker
  "Verifies scanner resource bounds, transient fault recovery, and pool/migration fencing:
    - Working-set cache/memory MUST stay bounded (`cache-entries <= cache-cap`, #8456/#8961).
    - Concurrency MUST recover above 1 after transient foreground `GetObject` (#8477).
    - Online local drive MUST recover from transient fault without restart (#9469/#9488).
    - Periodic status reads MUST NOT cause `namespace_lock_timeout` bursts (#9196/#9329).
    - >64 `part.N` probes MUST complete without truncation (backlog#2932).
    - Pool decommission/rebalance pause-backlog & source cleanup MUST leave 0 source orphans
      and 0 `FileCorrupt` listing errors (#7967/#8667/#7968/#8564).
    - On-demand migration background backfill MUST NOT resurrect deleted tombstones (backlog#2147)."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-pool-fence-verify))
            v (:value final-op)
            errors
            (cond-> []
              (nil? final-op)
              (conj {:type :missing-scanner-pool-fence-verify})

              (and v (> (or (:peak-cache-entries v) 0) (or (:cache-cap v) 1024)))
              (conj {:type :unbounded-scanner-cache-growth
                     :peak (:peak-cache-entries v)
                     :cap (:cache-cap v)})

              (and v (false? (:concurrency-recovered? v)))
              (conj {:type :scanner-concurrency-pinned-at-one})

              (and v (false? (:drive-recovered-without-restart? v)))
              (conj {:type :online-drive-stuck-faulty-until-restart})

              (and v (pos? (or (:status-read-lock-timeouts v) 0)))
              (conj {:type :scanner-status-namespace-lock-timeout :count (:status-read-lock-timeouts v)})

              (and v (false? (:over-64-parts-probed? v)))
              (conj {:type :bounded-part-probe-truncated})

              (and v (pos? (or (:source-orphans-after-cleanup v) 0)))
              (conj {:type :rebalance-source-cleanup-orphan :count (:source-orphans-after-cleanup v)})

              (and v (pos? (or (:list-file-corrupt-errors v) 0)))
              (conj {:type :decommission-list-file-corrupt :count (:list-file-corrupt-errors v)})

              (and v (pos? (or (:odm-resurrected-tombstones v) 0)))
              (conj {:type :odm-backfill-resurrected-tombstone :count (:odm-resurrected-tombstones v)}))]
        {:valid? (empty? errors)
         :errors errors}))))
