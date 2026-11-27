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
        (try
          (if (nil? expected)
            (do
              (s3/put-object! conn bucket key (pr-str replacement) :if-none-match "*")
              (assoc op :type :ok))
            (let [{:keys [body etag]} (s3/get-object! conn bucket key)
                  cur (edn/read-string body)]
              (if (= cur expected)
                (do
                  (s3/put-object! conn bucket key (pr-str replacement)
                                  :if-match (str "\"" etag "\""))
                  (assoc op :type :ok))
                (assoc op :type :fail :error :cas-value-mismatch))))
          (catch Throwable e
            (let [{:keys [class]} (s3/classify-exception e)]
              (if (#{:not-found :precondition} class)
                (assoc op :type :fail :error class)
                (handle-write-err op e))))))))

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
     :checker (rcheck/linearizable-register-checker)}))

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
        (try
          (let [existing (try
                           (let [{:keys [body etag]} (s3/get-object! conn bucket obj-key)]
                             {:list (edn/read-string body) :etag etag})
                           (catch Throwable e
                             (if (= :not-found (:class (s3/classify-exception e)))
                               nil
                               (throw e))))]
            (if existing
              (let [updated (conj (vec (:list existing)) v)]
                (s3/put-object! conn bucket obj-key (pr-str updated)
                                :if-match (str "\"" (:etag existing) "\""))
                (assoc op :type :ok :value [[:append k v]]))
              (do
                (s3/put-object! conn bucket obj-key (pr-str [v]) :if-none-match "*")
                (assoc op :type :ok :value [[:append k v]]))))
          (catch Throwable e
            (handle-write-err op e))))))

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
     :checker (rcheck/elle-append-checker)}))

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

      :final-list-versions
      (try
        (let [res (s3/list-versions! conn bucket :prefix "ver-")]
          (assoc op :type :ok :value res))
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
        get-gen (fn [_ _] {:type :invoke :f :get-version})]
    {:client (VersionedClient. nil (:bucket opts) recorded)
     :generator (gen/mix [put-gen get-gen])
     :final-generator (gen/once {:type :invoke :f :final-list-versions})
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
      (let [{:keys [key part1 part2]} (:value op)
            p1-bytes (.getBytes ^String part1 "UTF-8")
            p2-bytes (.getBytes ^String part2 "UTF-8")
            upload-id (atom nil)]
        (try
          (let [uid (s3/create-multipart! conn bucket key)
                _ (reset! upload-id uid)
                r1 (s3/upload-part! conn bucket key uid 1 p1-bytes)
                r2 (s3/upload-part! conn bucket key uid 2 p2-bytes)
                res (s3/complete-multipart! conn bucket key uid [r1 r2])]
            (assoc op :type :ok :value (merge (:value op) res)))
          (catch Throwable e
            (when-let [uid @upload-id]
              (try (s3/abort-multipart! conn bucket key uid) (catch Throwable _)))
            (handle-write-err op e))))

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
        ;; S3 requires non-final multipart parts to be at least 5 MiB (5,242,880 bytes).
        part1-size (:multipart-part-bytes opts (* 5 1024 1024))
        part1-pad (apply str (repeat (max 1 (- part1-size 32)) "R"))
        part2-pad (apply str (repeat 1024 "S"))
        mp-gen (fn [_ _]
                 (let [n (swap! seq-id inc)
                       k (str "mp-" (mod n key-count))
                       p1 (str (format "P1-%016d-" n) part1-pad)
                       p2 (str "P2-" n "-" part2-pad)
                       expected (s3/sha256-hex (str p1 p2))]
                   {:type :invoke
                    :f :complete-multipart
                    :value {:key k :part1 p1 :part2 p2 :expected-sha256 expected}}))
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

(def workloads
  {:register register-workload
   :append append-workload
   :versioned versioned-workload
   :multipart multipart-workload
   :list list-workload})
