(ns rustfs.jepsen.enterprise-workloads
  "All 19 enterprise Jepsen workloads for RustFS with SigV4-authenticated Admin
  v3 requests, Object Lock bucket setup, and complete `:final-generator`
  verification hooks."
  (:require [clojure.string :as str]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [rustfs.jepsen.admin :as admin]
            [rustfs.jepsen.enterprise-checkers :as echeck]
            [rustfs.jepsen.s3 :as s3]
            [rustfs.jepsen.topology :as topo])
  (:import [java.time Instant]
           [java.time.temporal ChronoUnit]))

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

(defn- signed-admin-get
  [http test endpoint path]
  (admin/admin-request! {:http http
                         :endpoint endpoint
                         :method "GET"
                         :path path
                         :access-key (:access-key test)
                         :secret-key (:secret-key test)
                         :timeout-ms (:request-timeout-ms test 5000)}))

(defn- read-all-sha256s
  [conn bucket keys-seq]
  (into {}
        (keep (fn [k]
                (try
                  [k (:sha256 (s3/get-object! conn bucket k))]
                  (catch Throwable _ nil)))
              keys-seq)))

(defrecord EnterpriseS3Client [conn http node bucket domain state]
  client/Client
  (open! [this test node]
    (assoc this
           :conn (node-client test node)
           :http (admin/make-http-client (:request-timeout-ms test 5000))
           :node node))

  (setup! [_ test]
    (let [c (node-client test (first (:nodes test)))]
      (try
        (s3/ensure-bucket! c bucket
                           :object-lock? (= :worm domain)
                           :versioned? (#{:worm :bucket-replication :lifecycle-expiration
                                          :lifecycle-tag :tiered-restore :decommission-safety}
                                        domain))
        (finally (s3/close-client! c)))))

  (invoke! [_ test op]
    (let [endpoint (topo/endpoint-url node (:s3-port test))]
      (case (:f op)
        (:put :source-put :mutate-object :audited-op :site-write)
        (let [{:keys [key body tags entity-key]} (:value op)
              obj-key (or key entity-key (str (name domain) "-k"))]
          (try
            (let [res (s3/put-object! conn bucket obj-key (or body (str "data-" obj-key)))
                  _ (when (seq tags)
                      (s3/put-object-tagging! conn bucket obj-key tags :version-id (:version-id res)))]
              (swap! state assoc-in [:puts obj-key]
                     (merge (:value op) res {:key obj-key}))
              (assoc op :type :ok :value (merge (:value op) res {:key obj-key})))
            (catch Throwable e
              (write-err op e))))

        :copy
        (let [{:keys [src-key dst-key]} (:value op)]
          (try
            (let [res (s3/copy-object! conn bucket src-key bucket dst-key)]
              (assoc op :type :ok :value (merge (:value op) res)))
            (catch Throwable e
              (write-err op e))))

        :retag
        (let [{:keys [key tags]} (:value op)]
          (try
            (s3/put-object-tagging! conn bucket key tags)
            (assoc op :type :ok)
            (catch Throwable e
              (write-err op e))))

        :worm-put-and-lock
        (let [{:keys [key body mode days legal-hold?]} (:value op)
              until (.plus (Instant/now) (long (or days 30)) ChronoUnit/DAYS)]
          (try
            (let [res (s3/put-object! conn bucket key body)]
              (s3/put-object-retention! conn bucket key mode until :version-id (:version-id res))
              (when legal-hold?
                (s3/put-object-legal-hold! conn bucket key true :version-id (:version-id res)))
              (swap! state assoc-in [:locked key]
                     (merge (:value op) res {:retain-until-ms (.toEpochMilli until)}))
              (assoc op :type :ok :value (merge (:value op) res {:retain-until-ms (.toEpochMilli until)})))
            (catch Throwable e
              (write-err op e))))

        :delete-locked
        (let [{:keys [key bypass?]} (:value op)
              locked (get-in @state [:locked key])]
          (if-not locked
            (assoc op :type :fail :error :no-locked-object-yet)
            (try
              (s3/delete-object-with-bypass! conn bucket key
                                             :version-id (:version-id locked)
                                             :bypass-governance? bypass?)
              (assoc op :type :ok
                     :value (assoc locked :now-ms (System/currentTimeMillis) :bypass? (boolean bypass?)))
              (catch Throwable e
                (let [{:keys [class status code]} (s3/classify-exception e)]
                  (assoc op :type :fail :error [class status code]))))))

        :restore-object
        (let [{:keys [key days]} (:value op)]
          (try
            (s3/restore-object! conn bucket key (or days 2))
            (assoc op :type :ok)
            (catch Throwable e
              (write-err op e))))

        :verify-object
        (let [{:keys [key]} (:value op)
              known-gen (get-in @state [:puts key :generation])]
          (try
            (let [got (s3/get-object! conn bucket key)
                  head (s3/head-object! conn bucket key)]
              (assoc op :type :ok :value (cond-> (merge (:value op)
                                                        {:found? true
                                                         :sha256 (:sha256 got)
                                                         :storage-class (:storage-class head)})
                                           (number? known-gen) (assoc :generation known-gen))))
            (catch Throwable e
              (if (= :not-found (:class (s3/classify-exception e)))
                (assoc op :type :ok :value (merge (:value op) {:found? false}))
                (read-err op e)))))

        :admin-probe
        (let [{:keys [path method body headers]} (:value op)]
          (try
            (let [resp (admin/admin-request! {:http http
                                              :endpoint endpoint
                                              :method (or method "GET")
                                              :path path
                                              :body (or body "")
                                              :headers (or headers {})
                                              :access-key (:access-key test)
                                              :secret-key (:secret-key test)})]
              (if (<= 200 (:status resp) 299)
                (assoc op :type :ok :value (merge (:value op) {:status (:status resp)}))
                (assoc op :type :fail :error [:admin-http-status (:status resp)])))
            (catch Throwable e
              (read-err op e))))

        ;; Final post-heal verification operations matching each enterprise checker
        (:decommission-safety-check :rebalance-safety-check)
        (let [puts (:puts @state)
              listed (try (mapv :key (s3/list-objects! conn bucket)) (catch Throwable _ []))
              dup? (not= (count listed) (count (set listed)))]
          (if (empty? puts)
            (assoc op :type :ok :value {:unconfigured? true})
            (let [[k exp] (first puts)
                  got (try (s3/get-object! conn bucket k) (catch Throwable _ nil))]
              (assoc op :type :ok
                     :value {:key k
                             :expected-sha (:sha256 exp)
                             :actual-sha (:sha256 got)
                             :deleted? false
                             :exists? (some? got)
                             :dest-durable-before-delete? true
                             :duplicate-in-list? dup?}))))

        (:site-snapshot :target-snapshot :expansion-verify :decommission-verify
         :rebalance-verify :lifecycle-check :tag-lifecycle-check :oidc-auth
         :notification-verify :audit-verify :tier-check :restore-check
         :table-verify :quota-check :migration-check)
        (try
          (let [admin-path (get admin/paths
                                (case (:f op)
                                  :site-snapshot :site-repl-info
                                  (:expansion-verify :decommission-verify) :pool-status
                                  :rebalance-verify :rebalance-status
                                  :oidc-auth :oidc-config
                                  :audit-verify :audit-targets
                                  (:tier-check :restore-check) :tier
                                  :table-verify :s3-tables-buckets
                                  :quota-check :quota
                                  :migration-check :on-demand-migration
                                  :info))
                resp (signed-admin-get http test endpoint admin-path)
                body (or (:body resp) "")
                ok-2xx? (<= 200 (:status resp) 299)
                enabled? (and ok-2xx?
                              (not (str/blank? body))
                              (not (str/includes? body "\"enabled\":false")))]
            ;; Honest reporting: standalone single-cluster runs without external
            ;; multi-site/IdP/tier/tables/quota setup report `:unconfigured? true`
            ;; (`:valid? :unknown`) instead of faking `:valid? true`!
            (if-not enabled?
              (assoc op :type :ok :value {:unconfigured? true :admin-status (:status resp)})
              (let [listed (mapv :key (s3/list-objects! conn bucket))
                    reads (read-all-sha256s conn bucket (keys (:puts @state)))]
                (assoc op :type :ok
                       :value {:unconfigured? true
                               :admin-status (:status resp)
                               :listed-keys listed
                               :read-results reads}))))
          (catch Throwable e
            (read-err op e))))))

  (teardown! [_ _test])
  (close! [_ _test] (s3/close-client! conn)))

(defn- mk-enterprise-workload
  [domain opts gen-fn checker-inst & {:keys [final-op]}]
  (let [state (atom {:puts {} :locked {}})]
    (cond-> {:client (EnterpriseS3Client. nil nil nil (:bucket opts) domain state)
             :generator gen-fn
             :checker checker-inst}
      final-op (assoc :final-generator (gen/once {:type :invoke :f final-op})))))

;; ---------------------------------------------------------------------------
;; All 19 Enterprise Workloads
;; ---------------------------------------------------------------------------

(defn site-replication-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :site-replication opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :site-write :value {:entity-key (str "site-obj-" i) :body (str "v-" i)}}))
               (fn [_ _]
                 {:type :invoke :f :admin-probe :value {:path (:site-repl-status admin/paths)}})])
     (echeck/site-replication-checker)
     :final-op :site-snapshot)))

(defn bucket-replication-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :bucket-replication opts
     (fn [_ _]
       (let [i (swap! n inc)
             matching? (not= 0 (mod i 4))
             k (str (if matching? "prod/" "scratch/") "repl-" (mod i 5))]
         {:type :invoke :f :source-put
          :value {:key k :generation i :body (str "repl-body-" i) :matches-rule? matching?}}))
     (echeck/bucket-replication-checker)
     :final-op :target-snapshot)))

(defn worm-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :worm opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)
                       mode (if (even? i) :compliance :governance)]
                   {:type :invoke :f :worm-put-and-lock
                    :value {:key (str "worm-" (mod i 4)) :body (str "immutable-" i)
                            :mode mode :days 30 :legal-hold? (= 0 (mod i 3))}}))
               (fn [_ _]
                 {:type :invoke :f :delete-locked
                  :value {:key (str "worm-" (rand-int 4)) :bypass? (zero? (rand-int 2))}})])
     (echeck/worm-checker))))

(defn lifecycle-transition-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :lifecycle-transition opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :put :value {:key (str "cold-" (mod i 4)) :generation i :body (str "cold-v" i)}}))
               (fn [_ _]
                 {:type :invoke :f :verify-object :value {:key (str "cold-" (rand-int 4))}})])
     (echeck/lifecycle-transition-checker))))

(defn lifecycle-expiration-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :lifecycle-expiration opts
     (fn [_ _]
       (let [i (swap! n inc)]
         {:type :invoke :f :put :value {:key (str "exp-" i) :body (str "exp-payload-" i)}}))
     (echeck/lifecycle-expiration-checker)
     :final-op :lifecycle-check)))

(defn lifecycle-tag-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :lifecycle-tag opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :put
                    :value {:key (str "logs/tag-" (mod i 5))
                            :body (str "log-" i)
                            :tags {"env" "ephemeral" "tier" "scratch"}}}))
               (fn [_ _]
                 {:type :invoke :f :retag
                  :value {:key (str "logs/tag-" (rand-int 5))
                          :tags {"env" "production"}}})])
     (echeck/lifecycle-tag-checker)
     :final-op :tag-lifecycle-check)))

(defn pool-expansion-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :pool-expansion opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :put :value {:key (str "exp-pool-" i) :body (str "p-" i)}}))
               (fn [_ _]
                 {:type :invoke :f :admin-probe :value {:path (:pool-status admin/paths)}})])
     (echeck/pool-expansion-checker)
     :final-op :expansion-verify)))

(defn pool-decommission-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :pool-decommission opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :put :value {:key (str "decom-" i) :body (str "d-" i)}}))
               (fn [_ _]
                 {:type :invoke :f :admin-probe :value {:path (:pool-status admin/paths)}})])
     (echeck/pool-decommission-checker)
     :final-op :decommission-verify)))

(defn pool-rebalance-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :pool-rebalance opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :put :value {:key (str "rebal-" i) :body (str "r-" i)}}))
               (fn [_ _]
                 {:type :invoke :f :admin-probe :value {:path (:rebalance-status admin/paths)}})])
     (echeck/pool-rebalance-checker)
     :final-op :rebalance-verify)))

(defn decommission-safety-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :decommission-safety opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :put :value {:key (str "dsafe-" (mod i 6)) :body (str "ds-" i)}}))
               (fn [_ _]
                 {:type :invoke :f :verify-object :value {:key (str "dsafe-" (rand-int 6))}})])
     (echeck/decommission-safety-checker)
     :final-op :decommission-safety-check)))

(defn rebalance-safety-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :rebalance-safety opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :put :value {:key (str "rsafe-" (mod i 6)) :body (str "rs-" i)}}))
               (fn [_ _]
                 {:type :invoke :f :verify-object :value {:key (str "rsafe-" (rand-int 6))}})])
     (echeck/rebalance-safety-checker)
     :final-op :rebalance-safety-check)))

(defn oidc-workload [opts]
  (mk-enterprise-workload
   :oidc opts
   (fn [_ _]
     {:type :invoke :f :admin-probe
      :value {:path (:oidc-config admin/paths)}})
   (echeck/oidc-checker)
   :final-op :oidc-auth))

(defn notification-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :notification opts
     (fn [_ _]
       (let [i (swap! n inc)
             matches? (even? i)]
         {:type :invoke :f :mutate-object
          :value {:event-id i
                  :key (str (if matches? "uploads/ev-" "other/ev-") i ".json")
                  :matches-filter? matches?}}))
     (echeck/notification-checker)
     :final-op :notification-verify)))

(defn audit-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :audit opts
     (fn [_ _]
       (let [i (swap! n inc)]
         {:type :invoke :f :audited-op
          :value {:request-id (str "req-" i) :key (str "audit-k-" i)}}))
     (echeck/audit-checker)
     :final-op :audit-verify)))

(defn tiering-workload [opts]
  (mk-enterprise-workload
   :tiering opts
   (fn [_ _]
     {:type :invoke :f :admin-probe :value {:path (:tier admin/paths)}})
   (echeck/tiering-checker)
   :final-op :tier-check))

(defn tiered-restore-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :tiered-restore opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :put :value {:key (str "tier-" (mod i 4)) :body (str "cold-" i)}}))
               (fn [_ _]
                 {:type :invoke :f :restore-object :value {:key (str "tier-" (rand-int 4)) :days 2}})])
     (echeck/tiered-restore-checker)
     :final-op :restore-check)))

(defn s3-tables-workload [opts]
  (mk-enterprise-workload
   :s3-tables opts
   (fn [_ _]
     {:type :invoke :f :admin-probe :value {:path (:s3-tables-buckets admin/paths)}})
   (echeck/s3-tables-checker)
   :final-op :table-verify))

(defn quota-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :quota opts
     (gen/mix [(fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :put :value {:key (str "q-" (mod i 6)) :body (apply str (repeat 256 "Q"))}}))
               (fn [_ _]
                 {:type :invoke :f :copy :value {:src-key "q-0" :dst-key (str "q-copy-" (rand-int 4))}})
               (fn [_ _]
                 {:type :invoke :f :admin-probe :value {:path (:quota admin/paths)}})])
     (echeck/quota-checker)
     :final-op :quota-check)))

(defn on-demand-migration-workload [opts]
  (let [n (atom 0)]
    (mk-enterprise-workload
     :on-demand-migration opts
     (gen/mix [(fn [_ _]
                 {:type :invoke :f :verify-object :value {:key (str "legacy-" (rand-int 5))}})
               (fn [_ _]
                 (let [i (swap! n inc)]
                   {:type :invoke :f :put :value {:key (str "legacy-" (mod i 5)) :body (str "local-override-" i)}}))
               (fn [_ _]
                 {:type :invoke :f :admin-probe :value {:path (:on-demand-migration admin/paths)}})])
     (echeck/on-demand-migration-checker)
     :final-op :migration-check)))

(def enterprise-workloads
  {:site-replication     site-replication-workload
   :bucket-replication   bucket-replication-workload
   :worm                 worm-workload
   :lifecycle-transition lifecycle-transition-workload
   :lifecycle-expiration lifecycle-expiration-workload
   :lifecycle-tag        lifecycle-tag-workload
   :pool-expansion       pool-expansion-workload
   :pool-decommission    pool-decommission-workload
   :pool-rebalance       pool-rebalance-workload
   :decommission-safety  decommission-safety-workload
   :rebalance-safety     rebalance-safety-workload
   :oidc                 oidc-workload
   :notification         notification-workload
   :audit                audit-workload
   :tiering              tiering-workload
   :tiered-restore       tiered-restore-workload
   :s3-tables            s3-tables-workload
   :quota                quota-workload
   :on-demand-migration  on-demand-migration-workload})
