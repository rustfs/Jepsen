(ns rustfs.jepsen.workloads
  "Five Jepsen consistency workloads for RustFS:
    - :register  -> Knossos linearizable single-object PUT / GET / DELETE / CAS
    - :append    -> Elle strict-serializable list-append over versioned S3 keys
    - :versioned -> S3 bucket versioning durability & non-resurrection
    - :multipart -> All-or-nothing multipart upload atomicity & integrity
    - :list      -> Read-after-write & ListObjectsV2 convergence"
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [rustfs.jepsen.checker :as rcheck]
            [rustfs.jepsen.enterprise-workloads :as ework]
            [rustfs.jepsen.s3 :as s3]
            [rustfs.jepsen.topology :as topo]))

(defn- make-node-client
  [test node]
  (s3/make-client {:endpoint (topo/endpoint-url node (:s3-port test))
                   :access-key (:access-key test)
                   :secret-key (:secret-key test)
                   :timeout-ms (:request-timeout-ms test 5000)}))

(defn- handle-write-err
  [op ^Throwable e]
  (let [{:keys [class message code status]} (s3/classify-exception e)]
    (if (#{:precondition :definite-fail} class)
      (assoc op :type :fail :error [class status code message])
      (assoc op :type :info :error [class status code message]))))

(defn- handle-read-err
  [op ^Throwable e]
  (let [{:keys [class message code status]} (s3/classify-exception e)]
    (assoc op :type :fail :error [class status code message])))

;; ---------------------------------------------------------------------------
;; 1. Linearizable Register + Conditional CAS Workload (Knossos)
;; ---------------------------------------------------------------------------

(defrecord RegisterClient [conn bucket key]
  client/Client
  (open! [this test node]
    (assoc this :conn (make-node-client test node)))

  (setup! [_ test]
    (let [c (make-node-client test (first (:nodes test)))]
      (try
        (s3/ensure-bucket! c bucket :versioned? false)
        (finally (s3/close-client! c)))))

  (invoke! [_ _test op]
    (case (:f op)
      :read
      (try
        (let [{:keys [body]} (s3/get-object! conn bucket key)]
          (assoc op :type :ok :value (edn/read-string body)))
        (catch Throwable e
          (let [{:keys [class]} (s3/classify-exception e)]
            (if (= class :not-found)
              (assoc op :type :ok :value nil)
              (handle-read-err op e)))))

      :write
      (try
        (s3/put-object! conn bucket key (pr-str (:value op)))
        (assoc op :type :ok)
        (catch Throwable e
          (handle-write-err op e)))

      :delete
      (try
        (s3/delete-object! conn bucket key)
        (assoc op :type :ok)
        (catch Throwable e
          (handle-write-err op e)))

      :cas
      (let [[expected replacement] (:value op)]
        ;; Only the conditional PUT is a write. A failed read means the PUT was
        ;; never sent, so the CAS definitely did not happen. Recording that as
        ;; :info lets Knossos explain an illegal later read as "the CAS landed".
        (if (nil? expected)
          (try
            (s3/put-object! conn bucket key (pr-str replacement) :if-none-match "*")
            (assoc op :type :ok)
            (catch Throwable e
              (handle-write-err op e)))
          (let [observed (try
                           (let [{:keys [body etag]} (s3/get-object! conn bucket key)]
                             {:value (edn/read-string body) :etag etag})
                           (catch Throwable e
                             (let [classified (s3/classify-exception e)]
                               (if (= :not-found (:class classified))
                                 :absent
                                 {:read-failed? true :classified classified}))))]
            (cond
              (= observed :absent)
              (assoc op :type :fail :error :not-found)

              (:read-failed? observed)
              (let [{:keys [class status code message]} (:classified observed)]
                (assoc op :type :fail :error [:cas-read-not-attempted class status code message]))

              (not= (:value observed) expected)
              (assoc op :type :fail :error :cas-value-mismatch)

              :else
              (try
                (s3/put-object! conn bucket key (pr-str replacement)
                                :if-match (str "\"" (:etag observed) "\""))
                (assoc op :type :ok)
                (catch Throwable e
                  (handle-write-err op e)))))))))

  (teardown! [_ _test])
  (close! [_ _test] (s3/close-client! conn)))

(defn register-workload
  [opts]
  (let [next-val (atom 0)
        r (fn [_ _] {:type :invoke :f :read})
        w (fn [_ _] {:type :invoke :f :write :value (swap! next-val inc)})
        d (fn [_ _] {:type :invoke :f :delete})
        cas (fn [_ _]
              (let [cur @next-val
                    exp (if (zero? cur) nil (rand-int (inc cur)))
                    rep (swap! next-val inc)]
                {:type :invoke :f :cas :value [exp rep]}))]
    {:client (RegisterClient. nil (:bucket opts) "jepsen-register")
     :generator (gen/mix [r r w cas d])
     :checker (checker/compose
               {:linearizable (rcheck/linearizable-register-checker)
                :conditional (rcheck/conditional-write-checker :cas)})}))

;; ---------------------------------------------------------------------------
;; 2. Elle List-Append Workload (Conditional S3 Append)
;; ---------------------------------------------------------------------------

(defrecord AppendClient [conn bucket]
  client/Client
  (open! [this test node]
    (assoc this :conn (make-node-client test node)))

  (setup! [_ test]
    (let [c (make-node-client test (first (:nodes test)))]
      (try
        (s3/ensure-bucket! c bucket :versioned? false)
        (finally (s3/close-client! c)))))

  (invoke! [_ _test op]
    (let [[[mop k v]] (:value op)
          obj-key (str "elle-k-" k)]
      (case mop
        :r
        (try
          (let [{:keys [body]} (s3/get-object! conn bucket obj-key)
                parsed (edn/read-string body)]
            (assoc op :type :ok :value [[:r k parsed]]))
          (catch Throwable e
            (let [{:keys [class]} (s3/classify-exception e)]
              (if (= class :not-found)
                (assoc op :type :ok :value [[:r k nil]])
                (handle-read-err op e)))))

        :append
        (let [existing (try
                         (let [{:keys [body etag]} (s3/get-object! conn bucket obj-key)]
                           {:list (edn/read-string body) :etag etag})
                         (catch Throwable e
                           (let [classified (s3/classify-exception e)]
                             (if (= :not-found (:class classified))
                               :absent
                               {:read-failed? true :classified classified}))))]
          (if (:read-failed? existing)
            (let [{:keys [class status code message]} (:classified existing)]
              (assoc op :type :fail :error [:append-read-not-attempted class status code message]))
            (try
              (if (= existing :absent)
                (s3/put-object! conn bucket obj-key (pr-str [v]) :if-none-match "*")
                (let [updated (conj (vec (:list existing)) v)]
                  (s3/put-object! conn bucket obj-key (pr-str updated)
                                  :if-match (str "\"" (:etag existing) "\""))))
              (assoc op :type :ok :value [[:append k v]])
              (catch Throwable e
                (handle-write-err op e))))))))

  (teardown! [_ _test])
  (close! [_ _test] (s3/close-client! conn)))

(defn append-workload
  [opts]
  (let [key-count (:key-count opts 4)
        next-v (atom 0)
        r (fn [_ _] {:type :invoke :f :txn :value [[:r (rand-int key-count) nil]]})
        a (fn [_ _] {:type :invoke :f :txn :value [[:append (rand-int key-count) (swap! next-v inc)]]})]
    {:client (AppendClient. nil (:bucket opts))
     :generator (gen/mix [r a])
     :checker (checker/compose
               {:elle (rcheck/elle-append-checker)
                :conditional (rcheck/conditional-write-checker :txn)})}))

;; ---------------------------------------------------------------------------
;; 3. S3 Bucket Versioning Durability Workload
;; ---------------------------------------------------------------------------

(defrecord VersionedClient [conn bucket recorded]
  client/Client
  (open! [this test node]
    (assoc this :conn (make-node-client test node)))

  (setup! [_ test]
    (let [c (make-node-client test (first (:nodes test)))]
      (try
        (s3/ensure-bucket! c bucket :versioned? true)
        (finally (s3/close-client! c)))))

  (invoke! [_ _test op]
    (case (:f op)
      :put-version
      (let [{:keys [key body]} (:value op)]
        (try
          (let [{:keys [version-id sha256]} (s3/put-object! conn bucket key body)]
            (when version-id
              (swap! recorded conj {:key key :version-id version-id :sha256 sha256}))
            (assoc op :type :ok :value {:key key :version-id version-id :sha256 sha256}))
          (catch Throwable e
            (handle-write-err op e))))

      :get-version
      (if-let [sample (when-let [items (seq @recorded)] (rand-nth items))]
        (try
          (let [{:keys [sha256]} (s3/get-object! conn bucket (:key sample)
                                                 :version-id (:version-id sample))]
            (assoc op :type :ok :value (assoc sample :found? true :sha256 sha256)))
          (catch Throwable e
            (let [{:keys [class]} (s3/classify-exception e)]
              (if (= class :not-found)
                (assoc op :type :ok :value (assoc sample :found? false))
                (handle-read-err op e)))))
        (assoc op :type :fail :error :no-recorded-versions-yet))

      :delete-marker
      (let [k (get-in op [:value :key])]
        ;; The key is chosen when the operation is invoked so an indeterminate
        ;; result still names the object. Otherwise a committed delete marker
        ;; whose response was lost looks like a lost current version.
        (if (str/blank? k)
          (assoc op :type :fail :error :no-recorded-versions-yet)
          (try
            (let [{:keys [version-id delete-marker?]}
                  (s3/delete-object! conn bucket k)]
              (assoc op :type :ok
                     :value {:key k
                             :version-id version-id
                             :delete-marker? (boolean delete-marker?)}))
            (catch Throwable e
              (handle-write-err op e)))))

      (:final-list-versions :final-get-current)
      (try
        (case (:f op)
          :final-list-versions
          (let [res (s3/list-versions! conn bucket :prefix "ver-")]
            (assoc op :type :ok :value res))

          :final-get-current
          (let [k (get-in op [:value :key])]
            (try
              (let [{:keys [sha256 version-id]} (s3/get-object! conn bucket k)]
                (assoc op :type :ok :value {:key k :found? true :sha256 sha256
                                            :version-id version-id}))
              (catch Throwable e
                (let [{:keys [class]} (s3/classify-exception e)]
                  (if (= class :not-found)
                    (assoc op :type :ok :value {:key k :found? false})
                    (handle-read-err op e)))))))
        (catch Throwable e
          (handle-read-err op e)))))

  (teardown! [_ _test])
  (close! [_ _test] (s3/close-client! conn)))

(defn versioned-workload
  [opts]
  (let [recorded (atom [])
        seq-id (atom 0)
        key-count (:key-count opts 4)
        put-gen (fn [_ _]
                  (let [n (swap! seq-id inc)
                        k (str "ver-" (mod n key-count))]
                    {:type :invoke :f :put-version :value {:key k :body (str "rustfs-version-" n)}}))
        get-gen (fn [_ _] {:type :invoke :f :get-version})
        del-gen (fn [_ _]
                  (let [sample (when-let [items (seq @recorded)] (rand-nth items))]
                    {:type :invoke :f :delete-marker :value {:key (:key sample)}}))]
    {:client (VersionedClient. nil (:bucket opts) recorded)
     :generator (gen/mix [put-gen put-gen get-gen del-gen])
     :final-generator (concat [{:type :invoke :f :final-list-versions}]
                              (map (fn [i]
                                     {:type :invoke
                                      :f :final-get-current
                                      :value {:key (str "ver-" i)}})
                                   (range key-count)))
     :checker (rcheck/versioned-checker)}))

;; ---------------------------------------------------------------------------
;; 4. Multipart Upload Atomicity Workload
;; ---------------------------------------------------------------------------

(defrecord MultipartClient [conn bucket]
  client/Client
  (open! [this test node]
    (assoc this :conn (make-node-client test node)))

  (setup! [_ test]
    (let [c (make-node-client test (first (:nodes test)))]
      (try
        (s3/ensure-bucket! c bucket :versioned? false)
        (finally (s3/close-client! c)))))

  (invoke! [_ _test op]
    (case (:f op)
      :complete-multipart
      (let [{:keys [key seed part1-size part2-size]} (:value op)
            p1 (s3/multipart-part-bytes seed 1 part1-size)
            p2 (s3/multipart-part-bytes seed 2 part2-size)
            upload-id (atom nil)
            complete-attempted? (atom false)]
        (try
          (let [uid (s3/create-multipart! conn bucket key)
                _ (reset! upload-id uid)
                r1 (s3/upload-part! conn bucket key uid 1 p1)
                r2 (s3/upload-part! conn bucket key uid 2 p2)]
            (reset! complete-attempted? true)
            (let [res (s3/complete-multipart! conn bucket key uid [r1 r2])]
              (assoc op :type :ok :value (merge (:value op) res))))
          (catch Throwable e
            (let [classified (s3/classify-exception e)
                  cls (:class classified)]
              ;; Abort only when CompleteMultipartUpload was not attempted, or
              ;; when it was definitely rejected. An indeterminate complete may
              ;; already be the visible object; aborting it races the commit.
              (when (and @upload-id
                         (or (not @complete-attempted?)
                             (#{:definite-fail :precondition :not-found} cls)))
                (try (s3/abort-multipart! conn bucket key @upload-id) (catch Throwable _)))
              (if @complete-attempted?
                (handle-write-err op e)
                (assoc op :type :fail :error [:multipart-before-complete cls
                                              (:status classified) (:code classified)
                                              (:message classified)]))))))

      (:get-object :final-get)
      (let [k (get-in op [:value :key])]
        (try
          (let [{:keys [sha256]} (s3/get-object! conn bucket k)]
            (assoc op :type :ok :value {:key k :found? true :sha256 sha256}))
          (catch Throwable e
            (let [{:keys [class]} (s3/classify-exception e)]
              (if (= class :not-found)
                (assoc op :type :ok :value {:key k :found? false})
                (handle-read-err op e))))))))

  (teardown! [_ _test])
  (close! [_ _test] (s3/close-client! conn)))

(defn multipart-workload
  [opts]
  (let [key-count (:key-count opts 3)
        seq-id (atom 0)
        ;; S3 and RustFS require every non-final part to be at least 5 MiB.
        ;; The part body is rebuilt from `seed` in the client so the history
        ;; does not store multi-megabyte payloads.
        part1-size (max s3/s3-min-part-size (:multipart-part-bytes opts s3/s3-min-part-size))
        part2-size 1024
        mp-gen (fn [_ _]
                 (let [n (swap! seq-id inc)
                       k (str "mp-" (mod n key-count))
                       expected (s3/sha256-hex-chunks
                                 [(s3/multipart-part-bytes n 1 part1-size)
                                  (s3/multipart-part-bytes n 2 part2-size)])]
                   {:type :invoke
                    :f :complete-multipart
                    :value {:key k
                            :seed n
                            :part1-size part1-size
                            :part2-size part2-size
                            :expected-sha256 expected}}))
        get-gen (fn [_ _]
                  {:type :invoke
                   :f :get-object
                   :value {:key (str "mp-" (rand-int key-count))}})
        final-gen (map (fn [i] {:type :invoke :f :final-get :value {:key (str "mp-" i)}})
                       (range key-count))]
    {:client (MultipartClient. nil (:bucket opts))
     :generator (gen/mix [mp-gen get-gen])
     :final-generator final-gen
     :checker (rcheck/multipart-checker)}))

;; ---------------------------------------------------------------------------
;; 5. Read-After-Write & ListObjectsV2 Convergence Workload
;; ---------------------------------------------------------------------------

(defrecord ListClient [conn bucket]
  client/Client
  (open! [this test node]
    (assoc this :conn (make-node-client test node)))

  (setup! [_ test]
    (let [c (make-node-client test (first (:nodes test)))]
      (try
        (s3/ensure-bucket! c bucket :versioned? false)
        (finally (s3/close-client! c)))))

  (invoke! [_ _test op]
    (case (:f op)
      :put
      (let [{:keys [key body]} (:value op)]
        (try
          (let [{:keys [sha256 etag]} (s3/put-object! conn bucket key body)]
            (assoc op :type :ok :value {:key key :sha256 sha256 :etag etag}))
          (catch Throwable e
            (handle-write-err op e))))

      :delete
      (let [{:keys [key]} (:value op)]
        (try
          (s3/delete-object! conn bucket key)
          (assoc op :type :ok :value {:key key})
          (catch Throwable e
            (handle-write-err op e))))

      :final-list
      (try
        (let [items (s3/list-objects! conn bucket :prefix "list-")]
          (assoc op :type :ok :value items))
        (catch Throwable e
          (handle-read-err op e)))))

  (teardown! [_ _test])
  (close! [_ _test] (s3/close-client! conn)))

(defn list-workload
  [opts]
  (let [seq-id (atom 0)
        put-gen (fn [_ _]
                  (let [n (swap! seq-id inc)
                        k (str "list-k-" n)]
                    {:type :invoke :f :put :value {:key k :body (str "payload-" n)}}))
        del-gen (fn [_ _]
                  (let [max-n @seq-id
                        k (str "list-k-" (if (pos? max-n) (inc (rand-int max-n)) 1))]
                    {:type :invoke :f :delete :value {:key k}}))]
    {:client (ListClient. nil (:bucket opts))
     :generator (gen/mix [put-gen put-gen del-gen])
     :final-generator (gen/once {:type :invoke :f :final-list})
     :checker (rcheck/list-consistency-checker)}))

(def core-workloads
  {:register register-workload
   :append append-workload
   :versioned versioned-workload
   :multipart multipart-workload
   :list list-workload})

(def workloads
  (merge core-workloads ework/enterprise-workloads))
