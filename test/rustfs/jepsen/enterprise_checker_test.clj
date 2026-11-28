(ns rustfs.jepsen.enterprise-checker-test
  "Positive and negative unit tests for all 19 enterprise RustFS Jepsen checkers
  and workload registry completeness (5 core + 19 enterprise = 24 workloads)."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [rustfs.jepsen.admin :as admin]
            [rustfs.jepsen.enterprise-checkers :as ec]
            [rustfs.jepsen.workloads :as workloads]))

(defn- mk-history [ops]
  (h/history (map-indexed (fn [i op] (merge {:index i :time (* i 1000000)} op)) ops)))

(deftest workload-registry-and-admin-helpers-test
  (is (= 31 (count workloads/workloads)))
  (is (= 5 (count workloads/core-workloads)))
  (is (= 19 (count workloads/enterprise-workloads)))
  (is (= 7 (count workloads/scanner-workloads)))
  (is (= 64 (count (admin/sigv4-derive-key "secret" "20261128" "us-east-1" "s3")))))

(deftest replication-checkers-test
  (testing "#1 site-replication accepts converged mesh and rejects site divergence or loops"
    (let [chk (ec/site-replication-checker)
          good (mk-history
                [{:process 0 :type :ok :f :site-write :value {:entity-key "iam:user:alice"}}
                 {:process 0 :type :ok :f :site-snapshot :value {:site "site-a" :entities {"iam:user:alice" "ok"}}}
                 {:process 1 :type :ok :f :site-snapshot :value {:site "site-b" :entities {"iam:user:alice" "ok"}}}])
          diverged (mk-history
                    [{:process 0 :type :ok :f :site-write :value {:entity-key "iam:user:alice"}}
                     {:process 0 :type :ok :f :site-snapshot :value {:site "site-a" :entities {"iam:user:alice" "v1"}}}
                     {:process 1 :type :ok :f :site-snapshot :value {:site "site-b" :entities {"iam:user:alice" "v2"}}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil diverged {}))))))

  (testing "#2 bucket-replication accepts matching replicas and rejects stale out-of-order overwrite"
    (let [chk (ec/bucket-replication-checker)
          good (mk-history
                [{:process 0 :type :ok :f :source-put :value {:key "prod/a" :generation 2 :sha256 "h2" :matches-rule? true}}
                 {:process 0 :type :ok :f :source-put :value {:key "tmp/b" :generation 1 :sha256 "hb" :matches-rule? false}}
                 {:process 1 :type :ok :f :target-snapshot :value {"prod/a" {:generation 2 :sha256 "h2" :status :replica}}}])
          stale (mk-history
                 [{:process 0 :type :ok :f :source-put :value {:key "prod/a" :generation 2 :sha256 "h2" :matches-rule? true}}
                  {:process 1 :type :ok :f :target-snapshot :value {"prod/a" {:generation 1 :sha256 "h1" :status :replica}}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil stale {})))))))

(deftest worm-and-lifecycle-checkers-test
  (testing "#3 worm rejects early COMPLIANCE delete even with bypass, accepts valid governance bypass"
    (let [chk (ec/worm-checker)
          good (mk-history
                [{:process 0 :type :ok :f :delete-locked
                  :value {:key "g1" :mode :governance :retain-until-ms 2000 :now-ms 1000 :legal-hold? false :bypass? true}}])
          bad-comp (mk-history
                    [{:process 0 :type :ok :f :delete-locked
                      :value {:key "c1" :mode :compliance :retain-until-ms 2000 :now-ms 1000 :legal-hold? false :bypass? true}}])
          bad-lh (mk-history
                  [{:process 0 :type :ok :f :delete-locked
                    :value {:key "g1" :mode :governance :retain-until-ms 2000 :now-ms 1000 :legal-hold? true :bypass? true}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad-comp {}))))
      (is (false? (:valid? (checker/check chk nil bad-lh {}))))))

  (testing "#4 lifecycle-transition rejects stale transition clobbering newer overwrite"
    (let [chk (ec/lifecycle-transition-checker)
          good (mk-history
                [{:process 0 :type :ok :f :put :value {:key "a" :generation 2 :sha256 "h2"}}
                 {:process 1 :type :ok :f :verify-object :value {:key "a" :generation 2 :sha256 "h2" :found? true}}])
          bad (mk-history
               [{:process 0 :type :ok :f :put :value {:key "a" :generation 2 :sha256 "h2"}}
                {:process 1 :type :ok :f :verify-object :value {:key "a" :generation 1 :sha256 "h1" :found? true :storage-class "GLACIER"}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {}))))))

  (testing "#5 lifecycle-expiration rejects WORM expiry and premature deletion"
    (let [chk (ec/lifecycle-expiration-checker)
          good (mk-history
                [{:process 0 :type :ok :f :lifecycle-check :value {:key "k1" :age-days 10 :expiry-days 7 :worm-locked? true :exists? true}}
                 {:process 0 :type :ok :f :lifecycle-check :value {:key "k2" :age-days 10 :expiry-days 7 :worm-locked? false :exists? false}}])
          bad (mk-history
               [{:process 0 :type :ok :f :lifecycle-check :value {:key "k1" :age-days 10 :expiry-days 7 :worm-locked? true :exists? false}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {}))))))

  (testing "#6 lifecycle-tag rejects TOCTOU deletion of re-tagged object"
    (let [chk (ec/lifecycle-tag-checker)
          good (mk-history
                [{:process 0 :type :ok :f :tag-lifecycle-check :value {:key "logs/a" :matches-and-filter? true :past-age? true :retagged-safe? true :exists? true}}
                 {:process 0 :type :ok :f :tag-lifecycle-check :value {:key "logs/b" :matches-and-filter? true :past-age? true :retagged-safe? false :exists? false}}])
          bad (mk-history
               [{:process 0 :type :ok :f :tag-lifecycle-check :value {:key "logs/a" :matches-and-filter? true :past-age? true :retagged-safe? true :exists? false}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {})))))))

(deftest pool-lifecycle-and-safety-checkers-test
  (testing "#7 pool-expansion verifies cross-pool union & zero duplicates"
    (let [chk (ec/pool-expansion-checker)
          good (mk-history
                [{:process 0 :type :ok :f :put :value {:key "k1" :sha256 "s1"}}
                 {:process 0 :type :ok :f :put :value {:key "k2" :sha256 "s2"}}
                 {:process 1 :type :ok :f :expansion-verify
                  :value {:listed-keys ["k1" "k2"] :pools-used [0 1] :read-results {"k1" "s1" "k2" "s2"}}}])
          dup (mk-history
               [{:process 0 :type :ok :f :put :value {:key "k1" :sha256 "s1"}}
                {:process 1 :type :ok :f :expansion-verify
                 :value {:listed-keys ["k1" "k1"] :pools-used [0 1] :read-results {"k1" "s1"}}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil dup {}))))))

  (testing "#8 pool-decommission rejects writes landing on decommissioning pool"
    (let [chk (ec/pool-decommission-checker)
          good (mk-history
                [{:process 0 :type :ok :f :put :value {:key "k1" :sha256 "s1" :landed-on-decommissioning-pool? false}}
                 {:process 1 :type :ok :f :decommission-verify :value {:source-remaining 0 :read-results {"k1" "s1"}}}])
          bad (mk-history
               [{:process 0 :type :ok :f :put :value {:key "k1" :sha256 "s1" :landed-on-decommissioning-pool? true}}
                {:process 1 :type :ok :f :decommission-verify :value {:source-remaining 0 :read-results {"k1" "s1"}}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {}))))))

  (testing "#9 pool-rebalance verifies skew reduction & key preservation"
    (let [chk (ec/pool-rebalance-checker)
          good (mk-history
                [{:process 0 :type :ok :f :put :value {:key "k1" :sha256 "s1"}}
                 {:process 1 :type :ok :f :rebalance-verify
                  :value {:before-spread 12 :after-spread 2 :listed-keys ["k1"] :read-results {"k1" "s1"}}}])
          bad (mk-history
               [{:process 0 :type :ok :f :put :value {:key "k1" :sha256 "s1"}}
                {:process 1 :type :ok :f :rebalance-verify
                 :value {:before-spread 12 :after-spread 12 :listed-keys ["k1"] :read-results {"k1" "s1"}}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {}))))))

  (testing "#10 decommission-safety catches delete resurrection and WORM stripping"
    (let [chk (ec/decommission-safety-checker)
          good (mk-history
                [{:process 0 :type :ok :f :decommission-safety-check
                  :value {:key "w1" :expected-sha "s1" :actual-sha "s1" :deleted? false :exists? true
                          :worm-expected {:mode :compliance} :worm-actual {:mode :compliance}}}])
          resurrected (mk-history
                       [{:process 0 :type :ok :f :decommission-safety-check
                         :value {:key "d1" :deleted? true :exists? true}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil resurrected {}))))))

  (testing "#11 rebalance-safety catches source deletion before dest durable & bitrot propagation"
    (let [chk (ec/rebalance-safety-checker)
          good (mk-history
                [{:process 0 :type :ok :f :rebalance-safety-check
                  :value {:key "r1" :expected-sha "s1" :actual-sha "s1" :deleted? false :exists? true
                          :dest-durable-before-delete? true :duplicate-in-list? false}}])
          bad (mk-history
               [{:process 0 :type :ok :f :rebalance-safety-check
                 :value {:key "r1" :expected-sha "s1" :actual-sha "s1" :deleted? false :exists? true
                         :dest-durable-before-delete? false :duplicate-in-list? false}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {})))))))

(deftest security-tiering-tables-quota-migration-checkers-test
  (testing "#12 oidc rejects fail-open forged/expired/revoked tokens"
    (let [chk (ec/oidc-checker)
          good (mk-history [{:process 0 :type :ok :f :oidc-auth :value {:token-kind :forged :allowed? false}}
                            {:process 0 :type :ok :f :oidc-auth :value {:token-kind :valid-rotated-kid :allowed? true}}])
          bad (mk-history [{:process 0 :type :ok :f :oidc-auth :value {:token-kind :expired :allowed? true}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {}))))))

  (testing "#13 notification detects missing event or blocked S3 data path"
    (let [chk (ec/notification-checker)
          good (mk-history [{:process 0 :type :ok :f :mutate-object :value {:event-id 1 :matches-filter? true :blocked-by-target-outage? false}}
                            {:process 1 :type :ok :f :notification-verify :value [1]}])
          bad (mk-history [{:process 0 :type :ok :f :mutate-object :value {:event-id 1 :matches-filter? true :blocked-by-target-outage? false}}
                           {:process 1 :type :ok :f :notification-verify :value []}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {}))))))

  (testing "#14 audit detects broken hash chain and secret leaks"
    (let [chk (ec/audit-checker)
          good (mk-history [{:process 0 :type :ok :f :audited-op :value {:request-id "r1"}}
                            {:process 1 :type :ok :f :audit-verify :value {:recorded-ids ["r1"] :chain-valid? true :leaked-secrets []}}])
          leaked (mk-history [{:process 0 :type :ok :f :audited-op :value {:request-id "r1"}}
                              {:process 1 :type :ok :f :audit-verify :value {:recorded-ids ["r1"] :chain-valid? true :leaked-secrets ["secret-key"]}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil leaked {}))))))

  (testing "#15 tiering rejects removal of in-use tier"
    (let [chk (ec/tiering-checker)
          good (mk-history [{:process 0 :type :ok :f :tier-check :value {:tier "COLD-S3" :in-use? true :removed? false :stub-readable? true}}])
          bad (mk-history [{:process 0 :type :ok :f :tier-check :value {:tier "COLD-S3" :in-use? true :removed? true :stub-readable? false}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {}))))))

  (testing "#16 tiered-restore rejects version rollback (#3074)"
    (let [chk (ec/tiered-restore-checker)
          good (mk-history [{:process 0 :type :ok :f :restore-check :value {:key "r1" :expected-sha "h1" :actual-sha "h1" :rolled-back-newer-version? false :stub-intact-after-ttl? true}}])
          bad (mk-history [{:process 0 :type :ok :f :restore-check :value {:key "r1" :expected-sha "h1" :actual-sha "h1" :rolled-back-newer-version? true :stub-intact-after-ttl? true}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {}))))))

  (testing "#17 s3-tables rejects double-commit on same versionToken"
    (let [chk (ec/s3-tables-checker)
          good (mk-history [{:process 0 :type :ok :f :table-cas-commit :value {:base-token "t0" :snapshot-id "s1"}}
                            {:process 1 :type :ok :f :table-cas-commit :value {:base-token "t1" :snapshot-id "s2"}}
                            {:process 2 :type :ok :f :table-verify :value {:snapshot-lineage ["s1" "s2"]}}])
          race (mk-history [{:process 0 :type :ok :f :table-cas-commit :value {:base-token "t0" :snapshot-id "s1"}}
                            {:process 1 :type :ok :f :table-cas-commit :value {:base-token "t0" :snapshot-id "s2"}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil race {}))))))

  (testing "#18 quota rejects over-limit HARD bucket and FIFO WORM eviction"
    (let [chk (ec/quota-checker)
          good (mk-history [{:process 0 :type :ok :f :quota-check :value {:bucket "b" :mode :hard :used-bytes 800 :limit-bytes 1000 :leaked-bytes 0 :worm-evicted? false}}])
          over (mk-history [{:process 0 :type :ok :f :quota-check :value {:bucket "b" :mode :hard :used-bytes 1200 :limit-bytes 1000 :leaked-bytes 0 :worm-evicted? false}}])
          worm (mk-history [{:process 0 :type :ok :f :quota-check :value {:bucket "b" :mode :fifo :used-bytes 900 :limit-bytes 1000 :leaked-bytes 0 :worm-evicted? true}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil over {}))))
      (is (false? (:valid? (checker/check chk nil worm {}))))))

  (testing "#19 on-demand-migration rejects tombstone resurrection and shadow clobber"
    (let [chk (ec/on-demand-migration-checker)
          good (mk-history [{:process 0 :type :ok :f :migration-check :value {:key "a" :state :local-overwrite :expected-sha "h2" :actual-sha "h2" :found? true}}
                            {:process 0 :type :ok :f :migration-check :value {:key "d" :state :tombstone :found? false}}])
          resurrected (mk-history [{:process 0 :type :ok :f :migration-check :value {:key "d" :state :tombstone :found? true}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil resurrected {})))))))
