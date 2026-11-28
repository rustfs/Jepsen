(ns rustfs.jepsen.composite-workloads
  "Composite multi-pool expansion + concurrent Decommission / Rebalance functional
  & chaos Jepsen workloads (`3x2 -> 6x2` and `4x4 -> 8x4`):
    - :expansion-decommission-all-features
    - :expansion-rebalance-all-features

  Lifecycle executed end-to-end against real RustFS clusters:
    1. Boot Pool 0 (`3x2` or `4x4`) as a single-pool cluster and seed baseline
       objects across 4 buckets (S3 CRUD/Copy/CAS/Multipart, Versioned, WORM
       COMPLIANCE + LegalHold, and Tagged objects).
    2. Live-expand the cluster to Pool 0 + Pool 1 (`6x2` or `8x4`) via
       `db/expand-cluster-pools!` and verify `/rustfs/admin/v3/pools/list`.
    3. Trigger background Pool 0 Decommission (`/rustfs/admin/v3/pools/decommission`)
       or Cross-Pool Rebalance (`/rustfs/admin/v3/rebalance/start`).
    4. Concurrently drive mixed S3 CRUD, Overwrite, Delete, CopyObject,
       Conditional `If-Match` CAS, Multipart Upload, Bucket Versioning, WORM
       Compliance/LegalHold enforcement, Object Tagging, and Admin v3
       scanner/usage/movement probes across all nodes while shards migrate.
    5. Verify post-movement convergence and bit-for-bit SHA-256 durability,
       non-resurrection of deletes, intact version chains, intact WORM retention,
       intact tags, and exact ListObjectsV2 results."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [rustfs.jepsen.admin :as admin]
            [rustfs.jepsen.db :as rdb]
            [rustfs.jepsen.s3 :as s3]
            [rustfs.jepsen.topology :as topo])
  (:import [java.time Instant]))

(defn- write-err
  [op ^Throwable e]
  (let [{:keys [class status code message]} (s3/classify-exception e)]
    (if (#{:precondition :definite-fail} class)
      (assoc op :type :fail :error [class status code message])
      (assoc op :type :info :error [class status code message]))))

(defn- read-err
  [op ^Throwable e]
  (let [{:keys [class status code message]} (s3/classify-exception e)]
    (assoc op :type :fail :error [class status code message])))

(defn- buckets-for
  [base]
  {:crud (str base "-crud")
   :ver  (str base "-ver")
   :worm (str base "-worm")
   :tag  (str base "-tag")})

(defn- do-multipart-upload!
  [conn bucket key seed]
  (let [p1 (s3/multipart-part-bytes seed 1 s3/s3-min-part-size)
        p2 (s3/multipart-part-bytes seed 2 65536)
        sha (s3/sha256-hex-chunks [p1 p2])
        uid (s3/create-multipart! conn bucket key)]
    (try
      (let [r1 (s3/upload-part! conn bucket key uid 1 p1)
            r2 (s3/upload-part! conn bucket key uid 2 p2)]
        (s3/complete-multipart! conn bucket key uid [r1 r2])
        {:key key :sha256 sha})
      (catch Throwable e
        (try (s3/abort-multipart! conn bucket key uid) (catch Throwable _))
        (throw e)))))

(defn- seed-pool0-and-expand!
  [test state base-bucket mode]
  (locking state
    (when-not (:expanded? @state)
      (let [pool0-node (first (:nodes test))
            endpoint (topo/endpoint-url pool0-node (:s3-port test))
            conn (s3/make-client {:endpoint endpoint
                                  :access-key (:access-key test)
                                  :secret-key (:secret-key test)
                                  :timeout-ms 15000})
            http (admin/make-http-client 10000)
            bkts (buckets-for base-bucket)]
        (try
          (info "Phase 1: Creating multi-feature buckets & seeding Pool 0 via" endpoint)
          (s3/ensure-bucket! conn (:crud bkts) :versioned? false)
          (s3/ensure-bucket! conn (:ver bkts) :versioned? true)
          (s3/ensure-bucket! conn (:worm bkts) :object-lock? true)
          (s3/ensure-bucket! conn (:tag bkts) :versioned? false)

          ;; 1. Seed 8 standard CRUD objects on Pool 0
          (doseq [i (range 8)]
            (let [k (str "seed-crud-" i)
                  body (str "pool0-seed-payload-" i "-" (apply str (repeat 128 "S")))
                  {:keys [sha256]} (s3/put-object! conn (:crud bkts) k body)]
              (swap! state assoc-in [:crud k] sha256)))

          ;; 2. Seed CAS hot register on Pool 0
          (let [{:keys [sha256]} (s3/put-object! conn (:crud bkts) "cas-hot" "cas-v0")]
            (swap! state assoc-in [:crud "cas-hot"] sha256))

          ;; 3. Seed 5 MiB+ Multipart object on Pool 0
          (let [{:keys [sha256]} (do-multipart-upload! conn (:crud bkts) "mp-seed-0" 1001)]
            (swap! state assoc-in [:crud "mp-seed-0"] sha256))

          ;; 4. Seed multi-version objects on Pool 0
          (doseq [k ["ver-seed-0" "ver-seed-1"]
                  v [1 2]]
            (let [body (str k "-version-" v "-" (apply str (repeat 64 "V")))
                  {:keys [version-id sha256]} (s3/put-object! conn (:ver bkts) k body)]
              (swap! state update :versions conj {:key k :version-id version-id :sha256 sha256})))

          ;; 5. Seed WORM Compliance + LegalHold objects on Pool 0
          (let [retain-until (.plusSeconds (Instant/now) 3600)]
            (doseq [k ["worm-seed-0" "worm-seed-1"]]
              (let [body (str "worm-immutable-" k)
                    {:keys [version-id sha256]} (s3/put-object! conn (:worm bkts) k body)]
                (s3/put-object-retention! conn (:worm bkts) k :compliance retain-until :version-id version-id)
                (s3/put-object-legal-hold! conn (:worm bkts) k true :version-id version-id)
                (swap! state assoc-in [:worm k] {:version-id version-id :sha256 sha256}))))

          ;; 6. Seed Tagged objects on Pool 0
          (doseq [[k tags] [["tag-seed-0" {"env" "production" "pool" "zero"}]
                            ["tag-seed-1" {"env" "staging" "tier" "hot"}]]]
            (let [body (str "tagged-" k)
                  {:keys [sha256]} (s3/put-object! conn (:tag bkts) k body)]
              (s3/put-object-tagging! conn (:tag bkts) k tags)
              (swap! state assoc-in [:tags k] {:sha256 sha256 :tags tags})))

          ;; Phase 2: Live expand cluster to Pool 0 + Pool 1 (`3x2 -> 6x2` or `4x4 -> 8x4`)
          (let [target-pools (max 2 (long (:pools test 2)))]
            (when (> (count (:nodes test))
                     (:nodes (get topo/topologies (:topology test))))
              (rdb/expand-cluster-pools! test target-pools)))

          ;; Verify Admin v3 pools/list & start Decommission or Rebalance!
          (let [pools-resp (admin/admin-request! {:http http
                                                  :endpoint endpoint
                                                  :path (:pools-list admin/paths)
                                                  :access-key (:access-key test)
                                                  :secret-key (:secret-key test)
                                                  :timeout-ms 10000})]
            (info "Admin v3 pools/list after expansion:" (:status pools-resp) (:body pools-resp))
            (swap! state assoc :pools-list-body (:body pools-resp) :pools-list-status (:status pools-resp)))

          (if (= mode :decommission)
            (let [resp (admin/start-pool-decommission! {:http http
                                                        :endpoint endpoint
                                                        :pool-id 0
                                                        :access-key (:access-key test)
                                                        :secret-key (:secret-key test)})]
              (info "Started Pool 0 Decommission:" (:status resp) (:body resp))
              (swap! state assoc :movement-started? true :movement-start-body (:body resp)))
            (let [resp (admin/start-pool-rebalance! {:http http
                                                     :endpoint endpoint
                                                     :access-key (:access-key test)
                                                     :secret-key (:secret-key test)})]
              (info "Started Cross-Pool Rebalance:" (:status resp) (:body resp))
              (swap! state assoc :movement-started? true :movement-start-body (:body resp))))

          (swap! state assoc :expanded? true)
          (finally
            (s3/close-client! conn)))))))

(defn- movement-settled?
  [mode body]
  (let [b (str/lower-case (str body))]
    (if (= mode :decommission)
      (or (str/includes? b "\"complete\"")
          (str/includes? b "\"completed\"")
          (str/includes? b "\"decommissioned\"")
          (str/includes? b "\"drained\""))
      (or (not (str/includes? b "\"started\""))
          (str/includes? b "\"completed\"")
          (str/includes? b "\"succeeded\"")
          (str/includes? b "\"stopped\"")))))

(defrecord ExpansionCompositeClient [conn http endpoint base-bucket mode state lock-obj]
  client/Client
  (open! [this test node]
    (let [ep (topo/endpoint-url node (:s3-port test))]
      (assoc this
             :endpoint ep
             :http (admin/make-http-client 8000)
             :conn (s3/make-client {:endpoint ep
                                    :access-key (:access-key test)
                                    :secret-key (:secret-key test)
                                    :timeout-ms (:request-timeout-ms test 10000)}))))

  (setup! [_ test]
    (seed-pool0-and-expand! test state base-bucket mode))

  (invoke! [_ test op]
    (let [bkts (buckets-for base-bucket)]
      (case (:f op)
        :crud-put
        (let [{:keys [key body]} (:value op)]
          (locking lock-obj
            (try
              (let [{:keys [sha256]} (s3/put-object! conn (:crud bkts) key body)]
                (swap! state (fn [s]
                               (-> s
                                   (assoc-in [:crud key] sha256)
                                   (update :deleted disj key))))
                (assoc op :type :ok :value {:key key :sha256 sha256}))
              (catch Throwable e
                (write-err op e)))))

        :crud-delete
        (let [{:keys [key]} (:value op)]
          (locking lock-obj
            (try
              (s3/delete-object! conn (:crud bkts) key)
              (swap! state (fn [s]
                             (-> s
                                 (update :crud dissoc key)
                                 (update :deleted (fnil conj #{}) key))))
              (assoc op :type :ok :value {:key key})
              (catch Throwable e
                (write-err op e)))))

        :crud-copy
        (let [{:keys [src-key dst-key]} (:value op)]
          (locking lock-obj
            (if-let [src-sha (get-in @state [:crud src-key])]
              (try
                (s3/copy-object! conn (:crud bkts) src-key (:crud bkts) dst-key)
                (swap! state (fn [s]
                               (-> s
                                   (assoc-in [:crud dst-key] src-sha)
                                   (update :deleted disj dst-key))))
                (assoc op :type :ok :value {:src-key src-key :dst-key dst-key :sha256 src-sha})
                (catch Throwable e
                  (write-err op e)))
              (assoc op :type :fail :error :source-absent))))

        :cas-update
        (let [{:keys [body]} (:value op)]
          (locking lock-obj
            (try
              (let [{:keys [etag]} (s3/get-object! conn (:crud bkts) "cas-hot")
                    {:keys [sha256]} (s3/put-object! conn (:crud bkts) "cas-hot" body
                                                     :if-match (str "\"" etag "\""))]
                (swap! state assoc-in [:crud "cas-hot"] sha256)
                (assoc op :type :ok :value {:key "cas-hot" :sha256 sha256}))
              (catch Throwable e
                (write-err op e)))))

        :multipart-put
        (let [{:keys [key seed]} (:value op)]
          (locking lock-obj
            (try
              (let [{:keys [sha256]} (do-multipart-upload! conn (:crud bkts) key seed)]
                (swap! state (fn [s]
                               (-> s
                                   (assoc-in [:crud key] sha256)
                                   (update :deleted disj key))))
                (assoc op :type :ok :value {:key key :sha256 sha256}))
              (catch Throwable e
                (write-err op e)))))

        :versioned-put
        (let [{:keys [key body]} (:value op)]
          (try
            (let [{:keys [version-id sha256]} (s3/put-object! conn (:ver bkts) key body)]
              (swap! state update :versions conj {:key key :version-id version-id :sha256 sha256})
              (assoc op :type :ok :value {:key key :version-id version-id :sha256 sha256}))
            (catch Throwable e
              (write-err op e))))

        :worm-verify-and-put
        (let [{:keys [key body]} (:value op)
              seed-worm (get-in @state [:worm "worm-seed-0"])]
          (try
            ;; 1. Verify existing WORM Compliance+LegalHold object CANNOT be deleted during movement!
            (let [delete-blocked?
                  (try
                    (s3/delete-object-with-bypass! conn (:worm bkts) "worm-seed-0"
                                                   :version-id (:version-id seed-worm)
                                                   :bypass-governance? true)
                    false
                    (catch Throwable e
                      (= :definite-fail (:class (s3/classify-exception e)))))
                  retain-until (.plusSeconds (Instant/now) 3600)
                  {:keys [version-id sha256]} (s3/put-object! conn (:worm bkts) key body)]
              (s3/put-object-retention! conn (:worm bkts) key :governance retain-until :version-id version-id)
              (s3/put-object-legal-hold! conn (:worm bkts) key true :version-id version-id)
              (swap! state assoc-in [:worm key] {:version-id version-id :sha256 sha256})
              (assoc op :type :ok :value {:key key
                                          :version-id version-id
                                          :sha256 sha256
                                          :worm-delete-blocked? delete-blocked?}))
            (catch Throwable e
              (write-err op e))))

        :tag-put-and-retag
        (let [{:keys [key body tags]} (:value op)]
          (locking lock-obj
            (try
              (let [{:keys [sha256]} (s3/put-object! conn (:tag bkts) key body)]
                (s3/put-object-tagging! conn (:tag bkts) key tags)
                (swap! state assoc-in [:tags key] {:sha256 sha256 :tags tags})
                (assoc op :type :ok :value {:key key :sha256 sha256 :tags tags}))
              (catch Throwable e
                (write-err op e)))))

        :admin-movement-probe
        (try
          (let [status-path (if (= mode :decommission)
                              (:pool-decommission admin/paths)
                              (:rebalance-status admin/paths))
                m-resp (admin/admin-request! {:http http
                                              :endpoint endpoint
                                              :path status-path
                                              :access-key (:access-key test)
                                              :secret-key (:secret-key test)})
                u-resp (admin/admin-request! {:http http
                                              :endpoint endpoint
                                              :path (:data-usage admin/paths)
                                              :access-key (:access-key test)
                                              :secret-key (:secret-key test)})]
            (assoc op :type :ok :value {:movement-status (:status m-resp)
                                        :usage-status (:status u-resp)}))
          (catch Throwable e
            (read-err op e)))

        :composite-final-verify
        (try
          (let [status-path (if (= mode :decommission)
                              (:pool-decommission admin/paths)
                              (:rebalance-status admin/paths))
                deadline (+ (System/currentTimeMillis) 60000)
                final-move-resp
                (loop []
                  (let [r (admin/admin-request! {:http http
                                                 :endpoint endpoint
                                                 :path status-path
                                                 :access-key (:access-key test)
                                                 :secret-key (:secret-key test)
                                                 :timeout-ms 10000})]
                    (if (or (movement-settled? mode (:body r))
                            (> (System/currentTimeMillis) deadline))
                      r
                      (do (Thread/sleep 1500) (recur)))))
                snap @state
                crud-mismatches
                (vec
                 (for [[k exp-sha] (:crud snap)
                       :let [got (try (:sha256 (s3/get-object! conn (:crud bkts) k))
                                      (catch Throwable e [:err (.getMessage e)]))]
                       :when (not= got exp-sha)]
                   {:key k :expected exp-sha :actual got}))
                resurrected
                (vec
                 (for [dk (:deleted snap)
                       :let [exists? (try (s3/get-object! conn (:crud bkts) dk) true
                                          (catch Throwable e
                                            (not= :not-found (:class (s3/classify-exception e)))))]
                       :when exists?]
                   dk))
                listed-crud (mapv :key (s3/list-objects! conn (:crud bkts)))
                version-mismatches
                (vec
                 (for [{:keys [key version-id sha256]} (:versions snap)
                       :let [got (try (:sha256 (s3/get-object! conn (:ver bkts) key :version-id version-id))
                                      (catch Throwable e [:err (.getMessage e)]))]
                       :when (not= got sha256)]
                   {:key key :version-id version-id :expected sha256 :actual got}))
                worm-failures
                (vec
                 (for [[k {:keys [version-id sha256]}] (:worm snap)
                       :let [got-sha (try (:sha256 (s3/get-object! conn (:worm bkts) k :version-id version-id))
                                          (catch Throwable _ nil))
                             blocked? (try
                                        (s3/delete-object-with-bypass! conn (:worm bkts) k
                                                                       :version-id version-id
                                                                       :bypass-governance? true)
                                        false
                                        (catch Throwable e
                                          (= :definite-fail (:class (s3/classify-exception e)))))]
                       :when (or (not= got-sha sha256) (not blocked?))]
                   {:key k :version-id version-id :sha-ok? (= got-sha sha256) :delete-blocked? blocked?}))
                tag-mismatches
                (vec
                 (for [[k {:keys [sha256 tags]}] (:tags snap)
                       :let [got-sha (try (:sha256 (s3/get-object! conn (:tag bkts) k))
                                          (catch Throwable _ nil))
                             got-tags (try (s3/get-object-tagging! conn (:tag bkts) k)
                                           (catch Throwable _ nil))]
                       :when (or (not= got-sha sha256) (not= got-tags tags))]
                   {:key k :expected-tags tags :actual-tags got-tags :sha-ok? (= got-sha sha256)}))]
            (assoc op :type :ok
                   :value {:mode mode
                           :movement-started? (boolean (:movement-started? snap))
                           :pools-list-status (:pools-list-status snap)
                           :final-movement-status (:status final-move-resp)
                           :final-movement-body (:body final-move-resp)
                           :checked-crud (count (:crud snap))
                           :checked-versions (count (:versions snap))
                           :checked-worm (count (:worm snap))
                           :checked-tags (count (:tags snap))
                           :crud-mismatches crud-mismatches
                           :resurrected resurrected
                           :duplicate-in-list? (not= (count listed-crud) (count (set listed-crud)))
                           :list-missing (vec (set/difference (set (keys (:crud snap))) (set listed-crud)))
                           :version-mismatches version-mismatches
                           :worm-failures worm-failures
                           :tag-mismatches tag-mismatches}))
          (catch Throwable e
            (read-err op e))))))

  (teardown! [_ _test])
  (close! [_ _test] (s3/close-client! conn)))

(defn expansion-movement-composite-checker
  "Validate all functional invariants across live multi-pool expansion + concurrent
  Decommission or Rebalance."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [worm-ops (filter #(and (= :ok (:type %)) (= :worm-verify-and-put (:f %))) history)
            mid-worm-leaks (filter #(false? (get-in % [:value :worm-delete-blocked?])) worm-ops)
            snaps (filter #(and (= :ok (:type %)) (= :composite-final-verify (:f %))) history)
            final-snap (last snaps)]
        (cond
          (seq mid-worm-leaks)
          {:valid? false
           :errors [{:type :worm-deleted-during-pool-movement :count (count mid-worm-leaks)}]}

          (nil? final-snap)
          {:valid? :unknown :reason :no-composite-final-verify}

          (not (:movement-started? (:value final-snap)))
          {:valid? :unknown :reason :pool-movement-not-started}

          :else
          (let [v (:value final-snap)
                errors
                (vec
                 (concat
                  (when-not (<= 200 (or (:pools-list-status v) 0) 299)
                    [{:type :pools-list-failed :status (:pools-list-status v)}])
                  (when-not (<= 200 (or (:final-movement-status v) 0) 299)
                    [{:type :movement-status-http-error :status (:final-movement-status v)}])
                  (when (seq (:crud-mismatches v))
                    [{:type :crud-or-multipart-or-cas-corruption :details (:crud-mismatches v)}])
                  (when (seq (:resurrected v))
                    [{:type :deleted-keys-resurrected :keys (:resurrected v)}])
                  (when (:duplicate-in-list? v)
                    [{:type :duplicate-keys-in-cross-pool-list}])
                  (when (seq (:list-missing v))
                    [{:type :missing-keys-in-cross-pool-list :keys (:list-missing v)}])
                  (when (seq (:version-mismatches v))
                    [{:type :versioned-history-corruption :details (:version-mismatches v)}])
                  (when (seq (:worm-failures v))
                    [{:type :worm-lock-or-payload-compromised :details (:worm-failures v)}])
                  (when (seq (:tag-mismatches v))
                    [{:type :object-tags-lost-or-corrupted :details (:tag-mismatches v)}])))]
            {:valid? (empty? errors)
             :summary (select-keys v [:mode :checked-crud :checked-versions :checked-worm :checked-tags
                                      :final-movement-status])
             :errors errors}))))))

(defn- build-composite-workload
  [mode opts]
  (let [seq-n (atom 0)
        mp-n (atom 2000)
        lock-obj (Object.)
        state (atom {:expanded? false
                     :crud {}
                     :deleted #{}
                     :versions []
                     :worm {}
                     :tags {}})]
    {:expand-from-single-pool? true
     :client (ExpansionCompositeClient. nil nil nil (:bucket opts) mode state lock-obj)
     :generator
     (gen/mix
      [(fn [_ _]
         (let [i (swap! seq-n inc)]
           {:type :invoke :f :crud-put
            :value {:key (str "live-crud-" (mod i 12))
                    :body (str "live-val-" i "-" (apply str (repeat 96 "L")))}}))
       (fn [_ _]
         {:type :invoke :f :crud-delete
          :value {:key (str "live-crud-" (rand-int 4))}})
       (fn [_ _]
         {:type :invoke :f :crud-copy
          :value {:src-key (str "seed-crud-" (rand-int 6))
                  :dst-key (str "copy-crud-" (rand-int 6))}})
       (fn [_ _]
         (let [i (swap! seq-n inc)]
           {:type :invoke :f :cas-update
            :value {:body (str "cas-v-" i)}}))
       (fn [_ _]
         (let [s (swap! mp-n inc)]
           {:type :invoke :f :multipart-put
            :value {:key (str "mp-live-" (mod s 3)) :seed s}}))
       (fn [_ _]
         (let [i (swap! seq-n inc)]
           {:type :invoke :f :versioned-put
            :value {:key (str "ver-live-" (mod i 4))
                    :body (str "ver-body-" i)}}))
       (fn [_ _]
         (let [i (swap! seq-n inc)]
           {:type :invoke :f :worm-verify-and-put
            :value {:key (str "worm-live-" (mod i 4))
                    :body (str "worm-body-" i)}}))
       (fn [_ _]
         (let [i (swap! seq-n inc)]
           {:type :invoke :f :tag-put-and-retag
            :value {:key (str "tag-live-" (mod i 4))
                    :body (str "tag-body-" i)
                    :tags {"env" (if (even? i) "production" "ephemeral")
                           "seq" (str i)}}}))
       (fn [_ _]
         {:type :invoke :f :admin-movement-probe})])
     :final-generator (gen/once {:type :invoke :f :composite-final-verify})
     :checker (expansion-movement-composite-checker)}))

(defn expansion-decommission-all-features-workload
  [opts]
  (build-composite-workload :decommission opts))

(defn expansion-rebalance-all-features-workload
  [opts]
  (build-composite-workload :rebalance opts))

(def composite-workloads
  {:expansion-decommission-all-features expansion-decommission-all-features-workload
   :expansion-rebalance-all-features    expansion-rebalance-all-features-workload})
