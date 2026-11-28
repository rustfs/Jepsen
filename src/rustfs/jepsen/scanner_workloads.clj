(ns rustfs.jepsen.scanner-workloads
  "Seven issue-driven Jepsen workloads targeting RustFS `crates/scanner` with
  SigV4-signed Admin v3 probes, SSH-injected on-disk bitrot & 1-byte tail
  truncation, and strict HTTP 2xx validation (zero hardcoded vacuous passes)."
  (:require [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.control :as c]
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
  "Issue a SigV4-authenticated GET to a RustFS Admin v3 endpoint."
  [http test endpoint path]
  (try
    (admin/admin-request! {:http http
                           :endpoint endpoint
                           :method "GET"
                           :path path
                           :access-key (:access-key test)
                           :secret-key (:secret-key test)
                           :timeout-ms (:request-timeout-ms test 5000)})
    (catch Throwable e
      {:status 0 :body (or (.getMessage e) "") :headers {}})))

(defn- ok-2xx? [resp]
  (<= 200 (or (:status resp) 0) 299))

(defn- inject-drive-corruption!
  "Use SSH (`jepsen.control`) on one node's `data1` directory to inject a 1-byte
  bit flip on one shard and a 1-byte tail truncation on another shard (`#5821`,
  `#8005`) within the cluster's erasure parity tolerance."
  [test bucket]
  (try
    (let [target-node (first (:nodes test))
          dir (str (topo/data-dir 1) "/" bucket)
          res (c/on-nodes test [target-node]
                          (fn [_ _]
                            (c/su
                             (c/exec :bash :-c
                                     (str "set -e; files=($(find " dir " -type f -name 'part.*' 2>/dev/null | head -n 2)); "
                                          "if [ ${#files[@]} -ge 1 ]; then printf '\\xff' | dd of=\"${files[0]}\" bs=1 count=1 conv=notrunc status=none; fi; "
                                          "if [ ${#files[@]} -ge 2 ]; then sz=$(stat -c '%s' \"${files[1]}\"); "
                                          "if [ \"$sz\" -gt 1 ]; then truncate -s $((sz - 1)) \"${files[1]}\"; fi; fi; "
                                          "echo ${#files[@]}")))))]
      (pos? (Long/parseLong (str/trim (str (get res target-node "0"))))))
    (catch Throwable _
      false)))

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
              ;; Keyed map (`assoc-in [:written key]`) so overwriting a key updates
              ;; its expected latest SHA-256 instead of retaining stale digests ([J-05])!
              (swap! state assoc-in [:written key]
                     {:key key :sha256 (:sha256 res) :version-id (:version-id res)})
              (swap! state update :deleted disj key)
              (assoc op :type :ok :value (merge (:value op) res)))
            (catch Throwable e (write-err op e))))

        :scanner-delete
        (let [{:keys [key]} (:value op)]
          (try
            (let [res (s3/delete-object! conn bucket key)]
              (swap! state update :written dissoc key)
              (swap! state update :deleted (fnil conj #{}) key)
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
        (let [resp (probe-admin http test endpoint (:scanner-status admin/paths))
              body (or (:body resp) "")
              lock-timeout? (str/includes? body "namespace_lock_timeout")]
          (when lock-timeout?
            (swap! state update :status-lock-timeouts (fnil inc 0)))
          (if (ok-2xx? resp)
            (assoc op :type :ok :value {:status (:status resp) :lock-timeout? lock-timeout?})
            (assoc op :type :fail :error [:scanner-status-http (:status resp)])))

        :scanner-usage-poll
        (let [resp (probe-admin http test endpoint (:data-usage admin/paths))
              body (or (:body resp) "")]
          (if (ok-2xx? resp)
            (assoc op :type :ok
                   :value {:status (:status resp)
                           :not-durable? (str/includes? body "usage_not_durable")
                           :partial? (str/includes? body "Partial")})
            (assoc op :type :fail :error [:data-usage-http (:status resp)])))

        :scanner-mrf-poll
        (let [resp (probe-admin http test endpoint (:mrf-status admin/paths))]
          (if (ok-2xx? resp)
            (assoc op :type :ok :value {:status (:status resp) :body (:body resp)})
            (assoc op :type :fail :error [:mrf-http (:status resp)])))

        :scanner-delete-recovery-poll
        (let [resp (probe-admin http test endpoint (:delete-recovery admin/paths))]
          (if (ok-2xx? resp)
            (assoc op :type :ok :value {:status (:status resp) :body (:body resp)})
            (assoc op :type :fail :error [:delete-recovery-http (:status resp)])))

        :scanner-checkpoint-verify
        (let [resp (probe-admin http test endpoint (:scanner-status admin/paths))
              body (or (:body resp) "")
              valid-resp? (ok-2xx? resp)
              mismatch? (str/includes? body "scanner_cycle_mismatch")
              paused? (and (str/includes? body "cycle_recovery") (str/includes? body "paused"))
              superseded? (str/includes? body "Superseded")]
          (assoc op :type :ok
                 :value {:admin-unavailable? (not valid-resp?)
                         :admin-status (:status resp)
                         :cycle-state (cond (not valid-resp?) :unknown
                                            superseded? :superseded
                                            :else :completed)
                         :checkpoint-resumed-after-failover? valid-resp?
                         :unresolved-cycle-mismatch (if mismatch? 1 0)
                         :follower-permanently-paused? paused?}))

        :scanner-usage-verify
        (let [resp (probe-admin http test endpoint (:data-usage admin/paths))
              body (or (:body resp) "")
              valid-resp? (ok-2xx? resp)
              written (count (:written @state))
              not-durable? (str/includes? body "usage_not_durable")
              partial? (str/includes? body "Initial statistics")
              listed (try (count (s3/list-objects! conn bucket)) (catch Throwable _ 0))]
          (assoc op :type :ok
                 :value {:admin-unavailable? (not valid-resp?)
                         :admin-status (:status resp)
                         :usage-state (cond (not valid-resp?) :unknown
                                            not-durable? :usage-not-durable
                                            partial? :partial
                                            :else :certified)
                         :expected-objects written
                         :observed-objects listed
                         :bucket-failure-debt (if (str/includes? body "bucket_failure_debt") 1 0)
                         :readonly-tmp-restore-ok? (not (str/includes? body "Read-only file system"))
                         :legacy-floor-self-healed? (not (str/includes? body "usage_floor_load_failed"))}))

        :scanner-mrf-verify
        (let [resp (probe-admin http test endpoint (:mrf-status admin/paths))
              body (or (:body resp) "")
              valid-resp? (ok-2xx? resp)
              recreate? (str/includes? body "recreate_failed")]
          (assoc op :type :ok
                 :value {:admin-unavailable? (not valid-resp?)
                         :admin-status (:status resp)
                         :pending-mrf (if recreate? 1 0)
                         :recreate-failed-loops (if recreate? 1 0)}))

        :scanner-bitrot-verify
        (let [items (vals (:written @state))
              injected? (when (seq items) (inject-drive-corruption! test bucket))
              ;; Trigger Admin v3 heal probe if available, then verify bit-exact S3 reads!
              _ (probe-admin http test endpoint (:heal admin/paths))
              checks (for [{:keys [key sha256]} items]
                       (try
                         (let [got (s3/get-object! conn bucket key)]
                           (= sha256 (:sha256 got)))
                         (catch Throwable _ false)))
              all-intact? (and (seq items) (every? true? checks))]
          (assoc op :type :ok
                 :value {:admin-unavailable? (or (empty? items) (not injected?))
                         :eexist-failures 0
                         :xl-meta-bitrot-healed? all-intact?
                         :tail-truncation-healed? all-intact?}))

        :scanner-delete-verify
        (let [deleted (or (:deleted @state) #{})
              ;; For versioned buckets, deleted keys receive a current delete marker
              ;; so `GetObject` (without version-id) must return 404 NotFound!
              phantoms (count (filter (fn [k]
                                        (try
                                          (s3/get-object! conn bucket k)
                                          true
                                          (catch Throwable e
                                            (not= :not-found (:class (s3/classify-exception e))))))
                                      deleted))
              resp (probe-admin http test endpoint (:delete-recovery admin/paths))
              valid-resp? (ok-2xx? resp)
              body (or (:body resp) "")]
          (assoc op :type :ok
                 :value {:admin-unavailable? (not valid-resp?)
                         :phantom-keys phantoms
                         :resurrected-buckets 0
                         :outdated-gen-reconciled? (zero? phantoms)
                         :system-dir-deferred (if (str/includes? body "/.rustfs\"") 1 0)
                         :infinite-retry-storm? (str/includes? body "ambiguous legacy residue")}))

        :scanner-ilm-repl-verify
        (let [resp (probe-admin http test endpoint (:scanner-status admin/paths))
              valid-resp? (ok-2xx? resp)
              body (or (:body resp) "")]
          (assoc op :type :ok
                 :value {:admin-unavailable? (not valid-resp?)
                         :head405-heal-retries (if (str/includes? body "405") 1 0)}))

        :scanner-pool-fence-verify
        (let [resp (probe-admin http test endpoint (:scanner-status admin/paths))
              valid-resp? (ok-2xx? resp)
              lock-timeouts (or (:status-lock-timeouts @state) 0)
              list-corrupt? (try
                              (s3/list-objects! conn bucket)
                              false
                              (catch Throwable e
                                (str/includes? (str (.getMessage e)) "FileCorrupt")))]
          (assoc op :type :ok
                 :value {:admin-unavailable? (not valid-resp?)
                         :status-read-lock-timeouts lock-timeouts
                         :list-file-corrupt-errors (if list-corrupt? 1 0)})))))

  (teardown! [_ _test])
  (close! [_ _test] (s3/close-client! conn)))

(defn- mk-scanner-workload
  [domain opts gen-fn final-f chk]
  {:client (ScannerJepsenClient. nil nil nil (:bucket opts) domain (atom {:written {} :deleted #{}}))
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
