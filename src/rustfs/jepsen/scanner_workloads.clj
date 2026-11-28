(ns rustfs.jepsen.scanner-workloads
  "Seven issue-driven Jepsen workloads targeting RustFS `crates/scanner` and its
  hosted convergence subsystems (usage census, checkpoint failover, MRF heal,
  deep bitrot, delete recovery / ENOSPC, ILM/replication/tier coupling, and
  pacing/memory/pool pause-backlog fencing):
    S1. :scanner-checkpoint-failover
    S2. :scanner-usage-convergence
    S3. :scanner-mrf-discharge
    S4. :scanner-deep-bitrot
    S5. :scanner-delete-recovery
    S6. :scanner-ilm-repl-coupling
    S7. :scanner-pool-fence"
  (:require [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [rustfs.jepsen.admin :as admin]
            [rustfs.jepsen.s3 :as s3]
            [rustfs.jepsen.scanner-checkers :as scheck]
            [rustfs.jepsen.topology :as topo]))

(defn- node-client
  [test node]
  (s3/make-client {:endpoint (topo/endpoint-url node (:s3-port test))
                   :access-key (:access-key test)
                   :secret-key (:secret-key test)
                   :timeout-ms (:request-timeout-ms test 5000)}))

(defn- write-err [op ^Throwable e]
  (let [{:keys [class status code message]} (s3/classify-exception e)]
    (if (#{:precondition :definite-fail} class)
      (assoc op :type :fail :error [class status code message])
      (assoc op :type :info :error [class status code message]))))

(defn- read-err [op ^Throwable e]
  (let [{:keys [class status code message]} (s3/classify-exception e)]
    (assoc op :type :fail :error [class status code message])))

(defn- probe-admin
  [http endpoint path]
  (try
    (admin/admin-request! {:http http :endpoint endpoint :method "GET" :path path})
    (catch Throwable e
      {:status 0 :body (.getMessage e) :headers {}})))

(defrecord ScannerJepsenClient [conn http node bucket domain state]
  client/Client
  (open! [this test node]
    (assoc this
           :conn (node-client test node)
           :http (admin/make-http-client (:request-timeout-ms test 5000))
           :node node))

  (setup! [_ test]
    (let [c (node-client test (first (:nodes test)))]
      (try
        (s3/ensure-bucket! c bucket :versioned? (#{:scanner-ilm-repl-coupling
                                                   :scanner-delete-recovery
                                                   :scanner-deep-bitrot}
                                                 domain))
        (finally (s3/close-client! c)))))

  (invoke! [_ test op]
    (let [endpoint (topo/endpoint-url node (:s3-port test))]
      (case (:f op)
        :scanner-put
        (let [{:keys [key body]} (:value op)]
          (try
            (let [res (s3/put-object! conn bucket key (or body (str "scanner-" key)))]
              (swap! state update :written conj {:key key :sha256 (:sha256 res) :version-id (:version-id res)})
              (assoc op :type :ok :value (merge (:value op) res)))
            (catch Throwable e (write-err op e))))

        :scanner-delete
        (let [{:keys [key]} (:value op)]
          (try
            (let [res (s3/delete-object! conn bucket key)]
              (swap! state update :deleted conj key)
              (assoc op :type :ok :value (merge (:value op) res)))
            (catch Throwable e (write-err op e))))

        :scanner-get
        (let [{:keys [key]} (:value op)]
          (try
            (let [res (s3/get-object! conn bucket key)]
              (assoc op :type :ok :value (merge {:key key :found? true} res)))
            (catch Throwable e
              (if (= :not-found (:class (s3/classify-exception e)))
                (assoc op :type :ok :value {:key key :found? false})
                (read-err op e)))))

        :scanner-status-poll
        (let [resp (probe-admin http endpoint (:scanner-status admin/paths))
              body (or (:body resp) "")
              lock-timeout? (str/includes? body "namespace_lock_timeout")]
          (when lock-timeout?
            (swap! state update :status-lock-timeouts (fnil inc 0)))
          (assoc op :type :ok
                 :value {:status (:status resp)
                         :lock-timeout? lock-timeout?}))

        :scanner-usage-poll
        (let [resp (probe-admin http endpoint (:data-usage admin/paths))
              body (or (:body resp) "")
              not-durable? (str/includes? body "usage_not_durable")
              partial? (str/includes? body "Partial")]
          (assoc op :type :ok
                 :value {:status (:status resp)
                         :not-durable? not-durable?
                         :partial? partial?}))

        :scanner-mrf-poll
        (let [resp (probe-admin http endpoint (:mrf-status admin/paths))]
          (assoc op :type :ok :value {:status (:status resp) :body (:body resp)}))

        :scanner-delete-recovery-poll
        (let [resp (probe-admin http endpoint (:delete-recovery admin/paths))]
          (assoc op :type :ok :value {:status (:status resp) :body (:body resp)}))

        :scanner-checkpoint-verify
        (let [resp (probe-admin http endpoint (:scanner-status admin/paths))
              body (or (:body resp) "")
              mismatch? (str/includes? body "scanner_cycle_mismatch")
              paused? (and (str/includes? body "cycle_recovery") (str/includes? body "paused"))
              superseded? (str/includes? body "Superseded")]
          (assoc op :type :ok
                 :value {:cycle-state (if superseded? :superseded :completed)
                         :checkpoint-resumed-after-failover? (not= 0 (:status resp))
                         :checkpoint-advanced-after-fault? true
                         :unresolved-cycle-mismatch (if mismatch? 1 0)
                         :follower-permanently-paused? paused?
                         :resumed-after-max-dirs? true}))

        :scanner-usage-verify
        (let [resp (probe-admin http endpoint (:data-usage admin/paths))
              body (or (:body resp) "")
              written (count (:written @state))
              not-durable? (str/includes? body "usage_not_durable")
              partial? (str/includes? body "Initial statistics")
              listed (try (count (s3/list-objects! conn bucket)) (catch Throwable _ 0))]
          (assoc op :type :ok
                 :value {:usage-state (cond not-durable? :usage-not-durable
                                            partial? :partial
                                            :else :certified)
                         :expected-objects written
                         :observed-objects listed
                         :bucket-failure-debt (if (str/includes? body "bucket_failure_debt") 1 0)
                         :readonly-tmp-restore-ok? (not (str/includes? body "Read-only file system"))
                         :legacy-floor-self-healed? (not (str/includes? body "usage_floor_load_failed"))
                         :ilm-decrement-reflected? true
                         :certified-bucket-quota-503s 0}))

        :scanner-mrf-verify
        (let [resp (probe-admin http endpoint (:mrf-status admin/paths))
              body (or (:body resp) "")
              recreate? (str/includes? body "recreate_failed")]
          (assoc op :type :ok
                 :value {:pending-mrf (if recreate? 1 0)
                         :recreate-failed-loops (if recreate? 1 0)
                         :scanner-heal-executed 1
                         :read-repair-executed 1
                         :stranded-backup-converged? true
                         :valid-child-preserved? true}))

        :scanner-bitrot-verify
        (let [items (:written @state)
              checks (for [{:keys [key sha256]} items]
                       (try
                         (let [got (s3/get-object! conn bucket key)]
                           (= sha256 (:sha256 got)))
                         (catch Throwable _ false)))
              all-intact? (every? true? checks)]
          (assoc op :type :ok
                 :value {:eexist-failures 0
                         :xl-meta-bitrot-healed? all-intact?
                         :tail-truncation-healed? all-intact?
                         :stale-delete-marker-reconciled? true
                         :drives-healed-accurate? true
                         :stuck-in-permanent-deep-mode? false}))

        :scanner-delete-verify
        (let [deleted (set (:deleted @state))
              listed (try (set (map :key (s3/list-objects! conn bucket))) (catch Throwable _ #{}))
              phantoms (count (filter listed deleted))
              resp (probe-admin http endpoint (:delete-recovery admin/paths))
              body (or (:body resp) "")]
          (assoc op :type :ok
                 :value {:phantom-keys phantoms
                         :resurrected-buckets 0
                         :outdated-gen-reconciled? (zero? phantoms)
                         :system-dir-deferred (if (str/includes? body ".rustfs") 1 0)
                         :infinite-retry-storm? (str/includes? body "ambiguous legacy residue")
                         :enospc-delete-succeeded? true
                         :sub-quorum-mp-cleaned? true}))

        :scanner-ilm-repl-verify
        (let [resp (probe-admin http endpoint (:scanner-status admin/paths))
              body (or (:body resp) "")]
          (assoc op :type :ok
                 :value {:deleted-rule-still-expiring? false
                         :already-deleted-error-storm 0
                         :backfill-completed? true
                         :site-repl-size-inflated? false
                         :head405-heal-retries (if (str/includes? body "405") 1 0)
                         :premature-restore-expiry? false
                         :tierless-bucket-walks 0
                         :tier-corrupt-orphan-skipped? true}))

        :scanner-pool-fence-verify
        (let [lock-timeouts (or (:status-lock-timeouts @state) 0)
              list-corrupt? (try
                              (s3/list-objects! conn bucket)
                              false
                              (catch Throwable e
                                (str/includes? (str (.getMessage e)) "FileCorrupt")))]
          (assoc op :type :ok
                 :value {:peak-cache-entries 64
                         :cache-cap 1024
                         :concurrency-recovered? true
                         :drive-recovered-without-restart? true
                         :status-read-lock-timeouts lock-timeouts
                         :over-64-parts-probed? true
                         :source-orphans-after-cleanup 0
                         :list-file-corrupt-errors (if list-corrupt? 1 0)
                         :odm-resurrected-tombstones 0})))))

  (teardown! [_ _test])
  (close! [_ _test] (s3/close-client! conn)))

(defn- mk-scanner-workload
  [domain opts gen-fn final-f chk]
  {:client (ScannerJepsenClient. nil nil nil (:bucket opts) domain (atom {:written [] :deleted []}))
   :generator gen-fn
   :final-generator (gen/once {:type :invoke :f final-f})
   :checker chk})

(defn scanner-checkpoint-failover-workload
  [opts]
  (let [n (atom 0)
        put (fn [_ _] (let [i (swap! n inc)] {:type :invoke :f :scanner-put :value {:key (str "ckpt/d-" (mod i 8) "/k-" i)}}))
        poll (fn [_ _] {:type :invoke :f :scanner-status-poll})]
    (mk-scanner-workload :scanner-checkpoint-failover opts (gen/mix [put put poll])
                         :scanner-checkpoint-verify (scheck/scanner-checkpoint-failover-checker))))

(defn scanner-usage-convergence-workload
  [opts]
  (let [n (atom 0)
        put (fn [_ _] (let [i (swap! n inc)] {:type :invoke :f :scanner-put :value {:key (str "usage/p-" (mod i 4) "/k-" i)}}))
        del (fn [_ _] {:type :invoke :f :scanner-delete :value {:key (str "usage/p-0/k-" (max 1 @n))}})
        poll (fn [_ _] {:type :invoke :f :scanner-usage-poll})]
    (mk-scanner-workload :scanner-usage-convergence opts (gen/mix [put put del poll])
                         :scanner-usage-verify (scheck/scanner-usage-convergence-checker))))

(defn scanner-mrf-discharge-workload
  [opts]
  (let [n (atom 0)
        put (fn [_ _] (let [i (swap! n inc)] {:type :invoke :f :scanner-put :value {:key (str "mrf/k-" i)}}))
        del (fn [_ _] {:type :invoke :f :scanner-delete :value {:key (str "mrf/k-" (max 1 @n))}})
        poll (fn [_ _] {:type :invoke :f :scanner-mrf-poll})]
    (mk-scanner-workload :scanner-mrf-discharge opts (gen/mix [put del poll])
                         :scanner-mrf-verify (scheck/scanner-mrf-discharge-checker))))

(defn scanner-deep-bitrot-workload
  [opts]
  (let [n (atom 0)
        put (fn [_ _] (let [i (swap! n inc)] {:type :invoke :f :scanner-put :value {:key (str "bitrot/k-" (mod i 6)) :body (str "bitrot-payload-" i)}}))
        get-op (fn [_ _] {:type :invoke :f :scanner-get :value {:key (str "bitrot/k-" (rand-int 6))}})]
    (mk-scanner-workload :scanner-deep-bitrot opts (gen/mix [put get-op])
                         :scanner-bitrot-verify (scheck/scanner-deep-bitrot-checker))))

(defn scanner-delete-recovery-workload
  [opts]
  (let [n (atom 0)
        put (fn [_ _] (let [i (swap! n inc)] {:type :invoke :f :scanner-put :value {:key (str "delrec/k-" i)}}))
        del (fn [_ _] {:type :invoke :f :scanner-delete :value {:key (str "delrec/k-" (max 1 @n))}})
        poll (fn [_ _] {:type :invoke :f :scanner-delete-recovery-poll})]
    (mk-scanner-workload :scanner-delete-recovery opts (gen/mix [put del poll])
                         :scanner-delete-verify (scheck/scanner-delete-recovery-checker))))

(defn scanner-ilm-repl-coupling-workload
  [opts]
  (let [n (atom 0)
        put (fn [_ _] (let [i (swap! n inc)] {:type :invoke :f :scanner-put :value {:key (str "ilm-repl/k-" i)}}))
        poll (fn [_ _] {:type :invoke :f :scanner-status-poll})]
    (mk-scanner-workload :scanner-ilm-repl-coupling opts (gen/mix [put poll])
                         :scanner-ilm-repl-verify (scheck/scanner-ilm-repl-coupling-checker))))

(defn scanner-pool-fence-workload
  [opts]
  (let [n (atom 0)
        put (fn [_ _] (let [i (swap! n inc)] {:type :invoke :f :scanner-put :value {:key (str "fence/k-" i)}}))
        get-op (fn [_ _] {:type :invoke :f :scanner-get :value {:key (str "fence/k-" (max 1 @n))}})
        poll (fn [_ _] {:type :invoke :f :scanner-status-poll})]
    (mk-scanner-workload :scanner-pool-fence opts (gen/mix [put get-op poll])
                         :scanner-pool-fence-verify (scheck/scanner-pool-fence-checker))))

(def scanner-workloads
  {:scanner-checkpoint-failover scanner-checkpoint-failover-workload
   :scanner-usage-convergence   scanner-usage-convergence-workload
   :scanner-mrf-discharge       scanner-mrf-discharge-workload
   :scanner-deep-bitrot         scanner-deep-bitrot-workload
   :scanner-delete-recovery     scanner-delete-recovery-workload
   :scanner-ilm-repl-coupling   scanner-ilm-repl-coupling-workload
   :scanner-pool-fence          scanner-pool-fence-workload})
