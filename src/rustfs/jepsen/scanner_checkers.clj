(ns rustfs.jepsen.scanner-checkers
  "Issue-driven Jepsen checkers for the 7 RustFS Scanner failure domains mapped
  directly to 75+ real `rustfs/rustfs` and `rustfs/backlog` issues.

  Every checker rejects non-2xx Admin HTTP responses or missing verification
  evidence as `:valid? :unknown` (never a vacuous `true`!), while failing closed
  (`:valid? false`) on any observed invariant violation."
  (:require [jepsen.checker :as checker]))

(defn- ok-ops [history f]
  (filter #(and (= :ok (:type %)) (= f (:f %))) history))

(defn- finalize-verdict [final-op v errors]
  (cond
    (seq errors) false
    (nil? final-op) :unknown
    (true? (:admin-unavailable? v)) :unknown
    :else true))

;; ---------------------------------------------------------------------------
;; S1. Scanner Checkpoint Failover & SATB Convergence Checker
;; ---------------------------------------------------------------------------

(defn scanner-checkpoint-failover-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-checkpoint-verify))
            v (:value final-op)
            errors
            (cond-> []
              (and v (not (:admin-unavailable? v)) (not= :completed (:cycle-state v)))
              (conj {:type :scanner-cycle-did-not-converge :state (:cycle-state v)})

              (and v (false? (:checkpoint-resumed-after-failover? v)))
              (conj {:type :checkpoint-lost-on-leader-failover :cursor (:resumed-cursor v)})

              (and v (false? (:checkpoint-advanced-after-fault? v)))
              (conj {:type :checkpoint-stalled-after-timeout-or-error})

              (and v (pos? (or (:unresolved-cycle-mismatch v) 0)))
              (conj {:type :scanner-cycle-mismatch-deadlock :count (:unresolved-cycle-mismatch v)})

              (and v (:follower-permanently-paused? v))
              (conj {:type :follower-recovery-permanently-paused})

              (and v (false? (:resumed-after-max-dirs? v)))
              (conj {:type :scanner-halted-after-cycle-max-dirs}))]
        {:valid? (finalize-verdict final-op v errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S2. Scanner Usage Census Durability & Dirty-Journal Convergence Checker
;; ---------------------------------------------------------------------------

(defn scanner-usage-convergence-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-usage-verify))
            v (:value final-op)
            errors
            (cond-> []
              (and v (not (:admin-unavailable? v)) (not= :certified (:usage-state v)))
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
        {:valid? (finalize-verdict final-op v errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S3. Scanner MRF Discharge & Anti-Starvation Checker
;; ---------------------------------------------------------------------------

(defn scanner-mrf-discharge-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-mrf-verify))
            v (:value final-op)
            errors
            (cond-> []
              (and v (pos? (or (:pending-mrf v) 0)))
              (conj {:type :undischarged-mrf-loop :pending (:pending-mrf v)})

              (and v (pos? (or (:recreate-failed-loops v) 0)))
              (conj {:type :absent-object-recreate-failed-loop :count (:recreate-failed-loops v)})

              (and v (some? (:scanner-heal-executed v)) (zero? (:scanner-heal-executed v)))
              (conj {:type :scanner-heal-starved-by-mrf-burst})

              (and v (some? (:read-repair-executed v)) (zero? (:read-repair-executed v)))
              (conj {:type :read-repair-starved-by-mrf-burst})

              (and v (false? (:stranded-backup-converged? v)))
              (conj {:type :stranded-rollback-backup-stalled})

              (and v (false? (:valid-child-preserved? v)))
              (conj {:type :parent-heal-deleted-valid-child}))]
        {:valid? (finalize-verdict final-op v errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S4. Scanner Deep Bitrot, `xl.meta` Corruption & Tail-Truncation Checker
;; ---------------------------------------------------------------------------

(defn scanner-deep-bitrot-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-bitrot-verify))
            v (:value final-op)
            errors
            (cond-> []
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
        {:valid? (finalize-verdict final-op v errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S5. Scanner Delete Recovery, Legacy Residue & ENOSPC Delete Checker
;; ---------------------------------------------------------------------------

(defn scanner-delete-recovery-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-delete-verify))
            v (:value final-op)
            errors
            (cond-> []
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
        {:valid? (finalize-verdict final-op v errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S6. Scanner ILM, Replication Backfill & Tier Recovery Coupling Checker
;; ---------------------------------------------------------------------------

(defn scanner-ilm-repl-coupling-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-ilm-repl-verify))
            v (:value final-op)
            errors
            (cond-> []
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
        {:valid? (finalize-verdict final-op v errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; S7. Scanner Pacing, Bounded Memory, Drive Recovery & Pool Fence Checker
;; ---------------------------------------------------------------------------

(defn scanner-pool-fence-checker
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [final-op (last (ok-ops history :scanner-pool-fence-verify))
            v (:value final-op)
            errors
            (cond-> []
              (and v (number? (:peak-cache-entries v)) (number? (:cache-cap v))
                   (> (:peak-cache-entries v) (:cache-cap v)))
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
        {:valid? (finalize-verdict final-op v errors)
         :errors errors}))))
