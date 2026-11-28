(ns rustfs.jepsen.enterprise-checkers
  "Dedicated Jepsen consistency, durability, security, and safety checkers for
  all 19 enterprise RustFS domains:
    1.  site-replication-checker
    2.  bucket-replication-checker
    3.  worm-checker
    4.  lifecycle-transition-checker
    5.  lifecycle-expiration-checker
    6.  lifecycle-tag-checker
    7.  pool-expansion-checker
    8.  pool-decommission-checker
    9.  pool-rebalance-checker
    10. decommission-safety-checker
    11. rebalance-safety-checker
    12. oidc-checker
    13. notification-checker
    14. audit-checker
    15. tiering-checker
    16. tiered-restore-checker
    17. s3-tables-checker
    18. quota-checker
    19. on-demand-migration-checker"
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [jepsen.checker :as checker]))

(defn- ok-ops [history f]
  (filter #(and (= :ok (:type %)) (= f (:f %))) history))

;; ---------------------------------------------------------------------------
;; 1. Site Replication Checker
;; ---------------------------------------------------------------------------

(defn site-replication-checker
  "Verifies multi-site active-active IAM/policy/bucket/object convergence:
    - All sites report identical final entity maps post-heal.
    - Zero non-conflicting acknowledged entities were lost.
    - Zero replication loop anomalies reported."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [writes (ok-ops history :site-write)
            expected-keys (set (map #(get-in % [:value :entity-key]) writes))
            snaps (ok-ops history :site-snapshot)
            by-site (reduce (fn [acc op]
                              (assoc acc (get-in op [:value :site]) (get-in op [:value :entities])))
                            {}
                            snaps)
            site-maps (vals by-site)
            converged? (and (seq site-maps) (apply = site-maps))
            missing (when (seq site-maps)
                      (set/difference expected-keys (set (keys (first site-maps)))))
            loops (filter #(get-in % [:value :loop-detected?]) (ok-ops history :site-sync))
            errors (cond-> []
                     (empty? site-maps) (conj {:type :no-final-snapshots})
                     (and (seq site-maps) (not converged?))
                     (conj {:type :site-divergence :sites (keys by-site)})
                     (seq missing) (conj {:type :lost-replicated-entities :missing (vec missing)})
                     (seq loops) (conj {:type :replication-loop-detected :count (count loops)}))]
        {:valid? (empty? errors)
         :sites-checked (count by-site)
         :tracked-entities (count expected-keys)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 2. Bucket Replication Checker
;; ---------------------------------------------------------------------------

(defn bucket-replication-checker
  "Verifies cross-cluster bucket replication:
    - Matching-prefix source PUTs arrive on target with identical SHA-256 and >= generation.
    - Delete markers replicate when enabled.
    - Non-matching prefixes are never replicated.
    - Target never regresses to an older generation (out-of-order guard) and never loops REPLICA."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [src-latest (reduce (fn [acc op]
                                 (let [{:keys [key generation sha256 matches-rule? delete-marker?]} (:value op)]
                                   (assoc acc key {:generation generation
                                                   :sha256 sha256
                                                   :matches-rule? matches-rule?
                                                   :delete-marker? (boolean delete-marker?)})))
                               {}
                               (ok-ops history :source-put))
            final-target (last (ok-ops history :target-snapshot))
            target-map (or (:value final-target) {})
            errors
            (vec
             (concat
              (when-not final-target [{:type :no-target-snapshot}])
              (for [[k {:keys [generation sha256 matches-rule? delete-marker?]}] src-latest
                    :let [t (get target-map k)]
                    :let [err (cond
                                (and matches-rule? (nil? t))
                                {:type :missing-replica :key k :expected-gen generation}

                                (and (not matches-rule?) (some? t))
                                {:type :unfiltered-replica :key k}

                                (and matches-rule? (< (:generation t) generation))
                                {:type :stale-target-generation :key k :expected generation :actual (:generation t)}

                                (and matches-rule? (not delete-marker?) (not= (:sha256 t) sha256))
                                {:type :replica-sha256-mismatch :key k :expected sha256 :actual (:sha256 t)}

                                (and matches-rule? delete-marker? (not (:delete-marker? t)))
                                {:type :missing-delete-marker :key k}

                                (and matches-rule? (not= :replica (:status t)))
                                {:type :missing-replica-status :key k :status (:status t)})]
                    :when err]
                err)))]
        {:valid? (empty? errors)
         :source-keys (count src-latest)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 3. WORM / Object Lock Checker
;; ---------------------------------------------------------------------------

(defn worm-checker
  "Verifies S3 Object Lock (COMPLIANCE, GOVERNANCE, Legal Hold):
    - COMPLIANCE version MUST NEVER be deleted before :retain-until-ms, even with bypass.
    - Legal Hold version MUST NEVER be deleted until hold is lifted.
    - GOVERNANCE without bypass MUST NEVER succeed before retention expiry.
    - COMPLIANCE retention MUST NEVER be shortened."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [errors
            (vec
             (for [op (filter #(= :ok (:type %)) history)
                   :let [{:keys [key mode retain-until-ms now-ms legal-hold? bypass?
                                 old-retain-until-ms new-retain-until-ms]} (:value op)
                         active? (and retain-until-ms now-ms (< now-ms retain-until-ms))]
                   :let [err (cond
                               (and (= :delete-locked (:f op)) legal-hold?)
                               {:type :legal-hold-breach :key key}

                               (and (= :delete-locked (:f op)) (= :compliance mode) active?)
                               {:type :compliance-early-delete :key key :now now-ms :until retain-until-ms}

                               (and (= :delete-locked (:f op)) (= :governance mode) active? (not bypass?))
                               {:type :governance-delete-without-bypass :key key}

                               (and (= :update-retention (:f op)) (= :compliance mode)
                                    old-retain-until-ms new-retain-until-ms
                                    (< new-retain-until-ms old-retain-until-ms))
                               {:type :compliance-retention-shortened :key key})]
                   :when err]
               err))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 4. Lifecycle Transition Checker
;; ---------------------------------------------------------------------------

(defn lifecycle-transition-checker
  "Verifies lifecycle cold-tier transition safety:
    - No transitioned object loses its bytes or mismatches SHA-256.
    - A concurrent client overwrite (higher generation) MUST NEVER be regressed
      back into a cold stub of an older generation."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [acked-puts (reduce (fn [acc op]
                                 (let [{:keys [key generation sha256]} (:value op)]
                                   (assoc acc key {:generation generation :sha256 sha256})))
                               {}
                               (ok-ops history :put))
            reads (ok-ops history :verify-object)
            errors
            (vec
             (for [op reads
                   :let [{:keys [key generation sha256 storage-class found?]} (:value op)
                         exp (get acked-puts key)]
                   :when exp
                   :let [err (cond
                               (not found?)
                               {:type :transition-lost-object :key key}

                               (< generation (:generation exp))
                               {:type :stale-transition-clobbered-overwrite
                                :key key :expected-gen (:generation exp) :actual-gen generation
                                :storage-class storage-class}

                               (not= sha256 (:sha256 exp))
                               {:type :transition-sha256-mismatch :key key})]
                   :when err]
               err))]
        {:valid? (empty? errors)
         :checked-reads (count reads)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 5. Lifecycle Expiration Checker
;; ---------------------------------------------------------------------------

(defn lifecycle-expiration-checker
  "Verifies lifecycle expiration rules:
    - No object is expired before its true monotonic expiration age.
    - WORM-locked versions are NEVER expired by lifecycle."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [errors
            (vec
             (for [op (ok-ops history :lifecycle-check)
                   :let [{:keys [key age-days expiry-days worm-locked? exists?]} (:value op)
                         err (cond
                               (and worm-locked? (not exists?))
                               {:type :worm-object-expired :key key}

                               (and (< age-days expiry-days) (not exists?))
                               {:type :premature-expiration :key key :age-days age-days :expiry-days expiry-days}

                               (and (not worm-locked?) (>= age-days expiry-days) exists?)
                               {:type :missed-expiration :key key :age-days age-days :expiry-days expiry-days})]
                   :when err]
               err))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 6. Tag-Based Lifecycle Management & Expiration Checker
;; ---------------------------------------------------------------------------

(defn lifecycle-tag-checker
  "Verifies tag-filtered lifecycle (`Prefix + Tag` AND-rules & TOCTOU safety):
    - Untagged, non-matching-tag, or re-tagged objects MUST survive sweeps.
    - Matching aged objects expire cleanly."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [errors
            (vec
             (for [op (ok-ops history :tag-lifecycle-check)
                   :let [{:keys [key matches-and-filter? past-age? exists? retagged-safe?]} (:value op)
                         err (cond
                               (and (not matches-and-filter?) (not exists?))
                               {:type :non-matching-tag-expired :key key}

                               (and retagged-safe? (not exists?))
                               {:type :toctou-retagged-object-deleted :key key}

                               (and matches-and-filter? past-age? (not retagged-safe?) exists?)
                               {:type :matching-tagged-object-not-expired :key key})]
                   :when err]
               err))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 7. Pool Expansion Checker
;; ---------------------------------------------------------------------------

(defn pool-expansion-checker
  "Verifies live pool expansion:
    - All pre-expansion and post-expansion keys remain readable with exact SHA-256.
    - Cross-pool ListObjectsV2 returns exact union with zero duplicates/missing keys.
    - Newly added pool admits writes."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [puts (reduce (fn [acc op]
                           (assoc acc (get-in op [:value :key]) (get-in op [:value :sha256])))
                         {}
                         (ok-ops history :put))
            final-snap (last (ok-ops history :expansion-verify))
            {:keys [listed-keys pools-used read-results]} (:value final-snap)
            errors
            (vec
             (concat
              (when-not final-snap [{:type :no-expansion-verify}])
              (when (and final-snap (< (count (set pools-used)) 2))
                [{:type :new-pool-unused :pools pools-used}])
              (when (and final-snap (not= (count listed-keys) (count (set listed-keys))))
                [{:type :duplicate-keys-in-cross-pool-list}])
              (when (and final-snap (not= (set listed-keys) (set (keys puts))))
                [{:type :cross-pool-list-mismatch
                  :missing (vec (set/difference (set (keys puts)) (set listed-keys)))
                  :extra (vec (set/difference (set listed-keys) (set (keys puts))))}])
              (for [[k sha] puts
                    :let [got (get read-results k)]
                    :when (and final-snap (not= got sha))]
                {:type :expansion-read-mismatch :key k :expected sha :actual got})))]
        {:valid? (empty? errors)
         :tracked-keys (count puts)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 8. Pool Decommission Checker
;; ---------------------------------------------------------------------------

(defn pool-decommission-checker
  "Verifies pool decommission lifecycle:
    - Decommissioning pool admits ZERO new writes.
    - Post-drain, 0 live objects remain on the decommissioned pool and 100% are
      readable bit-for-bit on surviving pools."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [puts (reduce (fn [acc op]
                           (assoc acc (get-in op [:value :key]) (get-in op [:value :sha256])))
                         {}
                         (ok-ops history :put))
            bad-placements (filter #(true? (get-in % [:value :landed-on-decommissioning-pool?]))
                                   (ok-ops history :put))
            final-snap (last (ok-ops history :decommission-verify))
            {:keys [source-remaining read-results]} (:value final-snap)
            errors
            (vec
             (concat
              (when-not final-snap [{:type :no-decommission-verify}])
              (when (seq bad-placements)
                [{:type :write-admitted-to-decommissioning-pool :count (count bad-placements)}])
              (when (and final-snap (pos? (or source-remaining 0)))
                [{:type :source-pool-not-fully-drained :remaining source-remaining}])
              (for [[k sha] puts
                    :let [got (get read-results k)]
                    :when (and final-snap (not= got sha))]
                {:type :lost-or-corrupt-after-decommission :key k :expected sha :actual got})))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 9. Pool Rebalance Checker
;; ---------------------------------------------------------------------------

(defn pool-rebalance-checker
  "Verifies cross-pool rebalance convergence & completeness."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [puts (reduce (fn [acc op]
                           (assoc acc (get-in op [:value :key]) (get-in op [:value :sha256])))
                         {}
                         (ok-ops history :put))
            final-snap (last (ok-ops history :rebalance-verify))
            {:keys [before-spread after-spread listed-keys read-results]} (:value final-snap)
            errors
            (vec
             (concat
              (when-not final-snap [{:type :no-rebalance-verify}])
              (when (and final-snap (>= after-spread before-spread))
                [{:type :rebalance-did-not-reduce-skew :before before-spread :after after-spread}])
              (when (and final-snap (not= (set listed-keys) (set (keys puts))))
                [{:type :rebalance-key-set-mismatch}])
              (for [[k sha] puts
                    :when (and final-snap (not= (get read-results k) sha))]
                {:type :rebalance-data-mismatch :key k})))]
        {:valid? (empty? errors)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 10. Decommission Data Consistency & Safety Checker
;; ---------------------------------------------------------------------------

(defn decommission-safety-checker
  "Verifies adversarial data safety during active pool decommission:
    - Zero overwrite clobber (newer client PUT wins over in-flight mover).
    - Zero delete resurrection (deleted key never reappears on destination).
    - WORM retention & Legal Hold preserved byte-for-byte on destination.
    - Crash between destination commit & source cleanup preserves exact SHA-256."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [errors
            (vec
             (for [op (ok-ops history :decommission-safety-check)
                   :let [{:keys [key expected-sha actual-sha deleted? exists?
                                 worm-expected worm-actual]} (:value op)
                         err (cond
                               (and deleted? exists?)
                               {:type :decommission-delete-resurrection :key key}

                               (and (not deleted?) (not exists?))
                               {:type :decommission-data-loss :key key}

                               (and (not deleted?) (not= expected-sha actual-sha))
                               {:type :decommission-overwrite-clobber-or-corrupt
                                :key key :expected expected-sha :actual actual-sha}

                               (and worm-expected (not= worm-expected worm-actual))
                               {:type :decommission-worm-stripped
                                :key key :expected worm-expected :actual worm-actual})]
                   :when err]
               err))]
        {:valid? (and (seq (ok-ops history :decommission-safety-check)) (empty? errors))
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 11. Rebalance Data Consistency & Safety Checker
;; ---------------------------------------------------------------------------

(defn rebalance-safety-checker
  "Verifies adversarial data safety during active pool rebalance:
    - Bitrot on source shard is RS-healed before committing to destination.
    - Concurrent overwrite/delete wins via generation barrier.
    - Destination commit strictly precedes source cleanup; zero duplicates."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [checks (ok-ops history :rebalance-safety-check)
            errors
            (vec
             (for [op checks
                   :let [{:keys [key expected-sha actual-sha deleted? exists?
                                 dest-durable-before-delete? duplicate-in-list?]} (:value op)
                         err (cond
                               (and deleted? exists?)
                               {:type :rebalance-delete-resurrection :key key}

                               (and (not deleted?) (not exists?))
                               {:type :rebalance-data-loss :key key}

                               (and (not deleted?) (not= expected-sha actual-sha))
                               {:type :rebalance-bitrot-or-clobber :key key}

                               (false? dest-durable-before-delete?)
                               {:type :rebalance-source-deleted-before-dest-durable :key key}

                               (true? duplicate-in-list?)
                               {:type :rebalance-duplicate-listing :key key})]
                   :when err]
               err))]
        {:valid? (and (seq checks) (empty? errors))
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 12. OIDC / STS AuthN & AuthZ Checker
;; ---------------------------------------------------------------------------

(defn oidc-checker
  "Verifies OIDC / STS security invariants:
    - Invalid/forged/expired/wrong-aud/wrong-iss/revoked tokens MUST NEVER succeed.
    - Valid tokens under active JWKS (including cached during transient IdP outage) succeed."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [checks (ok-ops history :oidc-auth)
            errors
            (vec
             (for [op checks
                   :let [{:keys [token-kind allowed?]} (:value op)
                         err (cond
                               (and (#{:forged :expired :not-yet-valid :wrong-aud :wrong-iss :revoked :retired-kid} token-kind)
                                    allowed?)
                               {:type :oidc-fail-open-security-breach :token-kind token-kind}

                               (and (#{:valid :valid-cached-idp-outage :valid-rotated-kid} token-kind)
                                    (not allowed?))
                               {:type :oidc-false-denial :token-kind token-kind})]
                   :when err]
               err))]
        {:valid? (and (seq checks) (empty? errors))
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 13. Bucket Notification Checker
;; ---------------------------------------------------------------------------

(defn notification-checker
  "Verifies bucket notification fan-out & backpressure isolation:
    - Every matching acknowledged mutation is delivered at-least-once post-recovery.
    - Non-matching prefix/suffix mutations are filtered out.
    - Target outages NEVER fail S3 data-path writes."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [expected (set (keep #(when (get-in % [:value :matches-filter?])
                                   (get-in % [:value :event-id]))
                                (ok-ops history :mutate-object)))
            blocked-puts (filter #(true? (get-in % [:value :blocked-by-target-outage?]))
                                 (ok-ops history :mutate-object))
            final-snap (last (ok-ops history :notification-verify))
            delivered (set (or (:value final-snap) []))
            missing (set/difference expected delivered)
            extra (set/difference delivered expected)
            errors (cond-> []
                     (nil? final-snap) (conj {:type :no-notification-verify})
                     (seq blocked-puts) (conj {:type :s3-datapath-blocked-by-notify-outage})
                     (seq missing) (conj {:type :missing-notification-events :missing (vec missing)})
                     (seq extra) (conj {:type :unfiltered-notification-events :extra (vec extra)}))]
        {:valid? (empty? errors)
         :expected-events (count expected)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 14. Audit Logging Checker
;; ---------------------------------------------------------------------------

(defn audit-checker
  "Verifies audit completeness, hash-chain tamper evidence, and secret redaction."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [expected-reqs (set (map #(get-in % [:value :request-id]) (ok-ops history :audited-op)))
            final-snap (last (ok-ops history :audit-verify))
            {:keys [recorded-ids chain-valid? leaked-secrets]} (:value final-snap)
            missing (set/difference expected-reqs (set recorded-ids))
            errors (cond-> []
                     (nil? final-snap) (conj {:type :no-audit-verify})
                     (seq missing) (conj {:type :missing-audit-records :missing (vec missing)})
                     (and final-snap (not chain-valid?)) (conj {:type :broken-audit-hash-chain})
                     (seq leaked-secrets) (conj {:type :audit-secret-leak :leaked leaked-secrets}))]
        {:valid? (empty? errors)
         :audited-ops (count expected-reqs)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 15. Tiering Management Checker
;; ---------------------------------------------------------------------------

(defn tiering-checker
  "Verifies remote tier lifecycle safety:
    - Deleting a tier referenced by any live stub MUST fail closed (`TierInUse`).
    - Credential rotation preserves stub readability."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [checks (ok-ops history :tier-check)
            errors
            (vec
             (for [op checks
                   :let [{:keys [tier in-use? removed? stub-readable?]} (:value op)
                         err (cond
                               (and in-use? removed?)
                               {:type :in-use-tier-deleted-dangling-stubs :tier tier}

                               (false? stub-readable?)
                               {:type :stub-unreadable-after-tier-rotation :tier tier})]
                   :when err]
               err))]
        {:valid? (and (seq checks) (empty? errors))
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 16. Tiered Remote Data Restore Checker (`RestoreObject`)
;; ---------------------------------------------------------------------------

(defn tiered-restore-checker
  "Verifies RestoreObject integrity, anti-rollback (#3074), and TTL expiration."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [checks (ok-ops history :restore-check)
            errors
            (vec
             (for [op checks
                   :let [{:keys [key expected-sha actual-sha rolled-back-newer-version?
                                 stub-intact-after-ttl?]} (:value op)
                         err (cond
                               (not= expected-sha actual-sha)
                               {:type :restore-sha256-mismatch :key key}

                               (true? rolled-back-newer-version?)
                               {:type :restore-rolled-back-newer-version :key key}

                               (false? stub-intact-after-ttl?)
                               {:type :cold-stub-corrupted-after-ttl :key key})]
                   :when err]
               err))]
        {:valid? (and (seq checks) (empty? errors))
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 17. S3 Tables Checker (Iceberg `versionToken` CAS Serializability)
;; ---------------------------------------------------------------------------

(defn s3-tables-checker
  "Verifies S3 Tables catalog & Iceberg optimistic concurrency control:
    - At most ONE writer commits on any given `base-version-token` (zero lost snapshots).
    - Deleting a non-empty namespace MUST fail closed.
    - Final snapshot lineage contains every acknowledged commit."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [commits (ok-ops history :table-cas-commit)
            by-base (group-by #(get-in % [:value :base-token]) commits)
            double-commits (for [[tok ops] by-base :when (> (count ops) 1)]
                             {:type :s3-tables-double-commit-same-token
                              :base-token tok :winners (count ops)})
            acked-snaps (set (map #(get-in % [:value :snapshot-id]) commits))
            final-snap (last (ok-ops history :table-verify))
            lineage (set (get-in final-snap [:value :snapshot-lineage]))
            missing-snaps (when final-snap (set/difference acked-snaps lineage))
            non-empty-ns-del (filter #(true? (get-in % [:value :deleted-non-empty-namespace?]))
                                     (ok-ops history :delete-namespace))
            errors (vec (concat double-commits
                                (when (seq missing-snaps)
                                  [{:type :lost-iceberg-snapshots :missing (vec missing-snaps)}])
                                (when (seq non-empty-ns-del)
                                  [{:type :non-empty-namespace-deleted}])))]
        {:valid? (empty? errors)
         :committed-snapshots (count acked-snaps)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 18. Capacity Quota Checker (`HARD` & `FIFO`)
;; ---------------------------------------------------------------------------

(defn quota-checker
  "Verifies distributed HARD and FIFO capacity quotas:
    - Used bytes NEVER exceed limit on HARD buckets (covering PUT, CopyObject, multipart).
    - Aborted multiparts and deletes reclaim reserved bytes with zero leak.
    - FIFO eviction NEVER deletes WORM-locked objects."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [checks (ok-ops history :quota-check)
            errors
            (vec
             (for [op checks
                   :let [{:keys [bucket mode used-bytes limit-bytes leaked-bytes worm-evicted?]} (:value op)
                         err (cond
                               (> used-bytes limit-bytes)
                               {:type :quota-limit-exceeded :bucket bucket :mode mode :used used-bytes :limit limit-bytes}

                               (pos? (or leaked-bytes 0))
                               {:type :quota-byte-leak :bucket bucket :leaked leaked-bytes}

                               (true? worm-evicted?)
                               {:type :fifo-evicted-worm-object :bucket bucket})]
                   :when err]
               err))]
        {:valid? (and (seq checks) (empty? errors))
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 19. On-Demand Migration Checker (Lazy Pull-Through + Tombstone Shadowing)
;; ---------------------------------------------------------------------------

(defn on-demand-migration-checker
  "Verifies on-demand lazy pull-through migration from legacy S3/MinIO:
    - Read-through returns bit-exact SHA-256 bytes from legacy source.
    - Local PutObject overwrites strictly shadow legacy source forever.
    - Local DeleteObject tombstones NEVER resurrect from legacy source."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [checks (ok-ops history :migration-check)
            errors
            (vec
             (for [op checks
                   :let [{:keys [key state expected-sha actual-sha found?]} (:value op)
                         err (cond
                               (and (= :tombstone state) found?)
                               {:type :migration-tombstone-resurrected :key key}

                               (and (#{:pulled :local-overwrite} state) (not found?))
                               {:type :migration-object-lost :key key}

                               (and (#{:pulled :local-overwrite} state) (not= expected-sha actual-sha))
                               {:type :migration-shadow-clobber-or-corrupt
                                :key key :state state :expected expected-sha :actual actual-sha})]
                   :when err]
               err))]
        {:valid? (and (seq checks) (empty? errors))
         :errors errors}))))
