(ns rustfs.jepsen.scanner-checker-test
  "Positive and negative unit tests for all 7 issue-driven RustFS Scanner Jepsen
  checkers, proving each catches real `rustfs/rustfs` failure regressions."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [rustfs.jepsen.admin :as admin]
            [rustfs.jepsen.scanner-checkers :as sc]
            [rustfs.jepsen.workloads :as workloads]))

(defn- mk-history [ops]
  (h/history (map-indexed (fn [i op] (merge {:index i :time (* i 1000000)} op)) ops)))

(deftest scanner-admin-routes-and-registry-test
  (is (= 7 (count workloads/scanner-workloads)))
  (is (= "/rustfs/admin/v3/scanner/status" (:scanner-status admin/paths)))
  (is (= "/rustfs/admin/v3/datausageinfo" (:data-usage admin/paths)))
  (is (= "/rustfs/admin/v3/heal/mrf" (:mrf-status admin/paths))))

(deftest scanner-checkpoint-failover-checker-test
  (let [chk (sc/scanner-checkpoint-failover-checker)
        base {:cycle-state :completed
              :checkpoint-resumed-after-failover? true
              :checkpoint-advanced-after-fault? true
              :unresolved-cycle-mismatch 0
              :follower-permanently-paused? false
              :resumed-after-max-dirs? true}]
    (testing "accepts converged SATB cycle with checkpoint failover & recovery"
      (is (true? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-checkpoint-verify :value base}]) {})))))
    (testing "rejects Superseded loop (#7108/#8494), lost failover checkpoint (#8461), mismatch (#8141), paused follower (#8297)"
      (doseq [bad [(assoc base :cycle-state :superseded)
                   (assoc base :checkpoint-resumed-after-failover? false)
                   (assoc base :checkpoint-advanced-after-fault? false)
                   (assoc base :unresolved-cycle-mismatch 1)
                   (assoc base :follower-permanently-paused? true)
                   (assoc base :resumed-after-max-dirs? false)]]
        (is (false? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-checkpoint-verify :value bad}]) {}))))))))

(deftest scanner-usage-convergence-checker-test
  (let [chk (sc/scanner-usage-convergence-checker)
        base {:usage-state :certified
              :expected-objects 20
              :observed-objects 20
              :bucket-failure-debt 0
              :readonly-tmp-restore-ok? true
              :legacy-floor-self-healed? true
              :ilm-decrement-reflected? true
              :certified-bucket-quota-503s 0}]
    (testing "accepts durable certified usage census"
      (is (true? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-usage-verify :value base}]) {})))))
    (testing "rejects usage_not_durable (#9520), 0 baseline (#9471), ENOENT debt (#9543), readonly /tmp (#9672), ILM leak (#8106), quota 503 (#8924)"
      (doseq [bad [(assoc base :usage-state :usage-not-durable)
                   (assoc base :observed-objects 0)
                   (assoc base :bucket-failure-debt 3)
                   (assoc base :readonly-tmp-restore-ok? false)
                   (assoc base :legacy-floor-self-healed? false)
                   (assoc base :ilm-decrement-reflected? false)
                   (assoc base :certified-bucket-quota-503s 2)]]
        (is (false? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-usage-verify :value bad}]) {}))))))))

(deftest scanner-mrf-discharge-checker-test
  (let [chk (sc/scanner-mrf-discharge-checker)
        base {:pending-mrf 0
              :recreate-failed-loops 0
              :scanner-heal-executed 8
              :read-repair-executed 8
              :stranded-backup-converged? true
              :valid-child-preserved? true}]
    (testing "accepts clean MRF discharge and unstarved heal lanes"
      (is (true? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-mrf-verify :value base}]) {})))))
    (testing "rejects undischarged MRF (#9629/#9370), recreate_failed (#7997), starvation (#7951), stranded bkp (#8608), deleted child (#9154)"
      (doseq [bad [(assoc base :pending-mrf 4)
                   (assoc base :recreate-failed-loops 1)
                   (assoc base :scanner-heal-executed 0)
                   (assoc base :read-repair-executed 0)
                   (assoc base :stranded-backup-converged? false)
                   (assoc base :valid-child-preserved? false)]]
        (is (false? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-mrf-verify :value bad}]) {}))))))))

(deftest scanner-deep-bitrot-checker-test
  (let [chk (sc/scanner-deep-bitrot-checker)
        base {:eexist-failures 0
              :xl-meta-bitrot-healed? true
              :tail-truncation-healed? true
              :stale-delete-marker-reconciled? true
              :drives-healed-accurate? true
              :stuck-in-permanent-deep-mode? false}]
    (testing "accepts full deep bitrot + tail truncation + stale delete marker heal"
      (is (true? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-bitrot-verify :value base}]) {})))))
    (testing "rejects EEXIST (#5821), unhealed xl.meta (#5837), missed 1-byte truncation (#8005), stale DM (#8007), permanent Deep (#8015)"
      (doseq [bad [(assoc base :eexist-failures 1)
                   (assoc base :xl-meta-bitrot-healed? false)
                   (assoc base :tail-truncation-healed? false)
                   (assoc base :stale-delete-marker-reconciled? false)
                   (assoc base :drives-healed-accurate? false)
                   (assoc base :stuck-in-permanent-deep-mode? true)]]
        (is (false? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-bitrot-verify :value bad}]) {}))))))))

(deftest scanner-delete-recovery-checker-test
  (let [chk (sc/scanner-delete-recovery-checker)
        base {:phantom-keys 0
              :resurrected-buckets 0
              :outdated-gen-reconciled? true
              :system-dir-deferred 0
              :infinite-retry-storm? false
              :enospc-delete-succeeded? true
              :sub-quorum-mp-cleaned? true}]
    (testing "accepts crash-consistent deletes, bounded recovery, and ENOSPC DeleteObject"
      (is (true? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-delete-verify :value base}]) {})))))
    (testing "rejects phantoms (#8464), resurrected bucket (#9647), .rustfs defer (#9594), retry storm (#9517), ENOSPC 503 (#9461)"
      (doseq [bad [(assoc base :phantom-keys 1)
                   (assoc base :resurrected-buckets 1)
                   (assoc base :outdated-gen-reconciled? false)
                   (assoc base :system-dir-deferred 1)
                   (assoc base :infinite-retry-storm? true)
                   (assoc base :enospc-delete-succeeded? false)
                   (assoc base :sub-quorum-mp-cleaned? false)]]
        (is (false? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-delete-verify :value bad}]) {}))))))))

(deftest scanner-ilm-repl-coupling-checker-test
  (let [chk (sc/scanner-ilm-repl-coupling-checker)
        base {:deleted-rule-still-expiring? false
              :already-deleted-error-storm 0
              :backfill-completed? true
              :site-repl-size-inflated? false
              :head405-heal-retries 0
              :premature-restore-expiry? false
              :tierless-bucket-walks 0
              :tier-corrupt-orphan-skipped? true}]
    (testing "accepts clean scanner ILM/replication/tier coupling"
      (is (true? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-ilm-repl-verify :value base}]) {})))))
    (testing "rejects deleted ILM resurrection (#8620), backfill abandon (#8768), HEAD 405 loop (#7959), restore race (#8834), tierless walk (#8758)"
      (doseq [bad [(assoc base :deleted-rule-still-expiring? true)
                   (assoc base :already-deleted-error-storm 31000)
                   (assoc base :backfill-completed? false)
                   (assoc base :site-repl-size-inflated? true)
                   (assoc base :head405-heal-retries 12)
                   (assoc base :premature-restore-expiry? true)
                   (assoc base :tierless-bucket-walks 5)
                   (assoc base :tier-corrupt-orphan-skipped? false)]]
        (is (false? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-ilm-repl-verify :value bad}]) {}))))))))

(deftest scanner-pool-fence-checker-test
  (let [chk (sc/scanner-pool-fence-checker)
        base {:peak-cache-entries 64
              :cache-cap 1024
              :concurrency-recovered? true
              :drive-recovered-without-restart? true
              :status-read-lock-timeouts 0
              :over-64-parts-probed? true
              :source-orphans-after-cleanup 0
              :list-file-corrupt-errors 0
              :odm-resurrected-tombstones 0}]
    (testing "accepts bounded scanner memory, concurrency recovery, and clean pool/migration fence"
      (is (true? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-pool-fence-verify :value base}]) {})))))
    (testing "rejects O(N) cache growth (#8456), pinned concurrency (#8477), stuck faulty drive (#9469), lock timeout (#9196), FileCorrupt (#8564)"
      (doseq [bad [(assoc base :peak-cache-entries 50000)
                   (assoc base :concurrency-recovered? false)
                   (assoc base :drive-recovered-without-restart? false)
                   (assoc base :status-read-lock-timeouts 3)
                   (assoc base :over-64-parts-probed? false)
                   (assoc base :source-orphans-after-cleanup 2)
                   (assoc base :list-file-corrupt-errors 1)
                   (assoc base :odm-resurrected-tombstones 1)]]
        (is (false? (:valid? (checker/check chk nil (mk-history [{:process 0 :type :ok :f :scanner-pool-fence-verify :value bad}]) {}))))))))
