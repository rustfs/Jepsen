(ns rustfs.jepsen.checker
  "Consistency, linearizability, serializability, versioning, multipart
  atomicity, and listing convergence checkers for RustFS Jepsen runs.

  Reuses official Jepsen verification engines:
    - Knossos (`jepsen.checker/linearizable`) for single-object S3 register + CAS
    - Elle (`elle.list-append/check`) for transactional anomaly detection
      (G0, G1a, G1b, G1c, G-single, G2-item)"
  (:require [elle.list-append :as elle-list]
            [jepsen.checker :as checker]
            [knossos.model :as model])
  (:import [knossos.model Model]))

;; ---------------------------------------------------------------------------
;; 1. Knossos Linearizable S3 Register + Delete + Conditional CAS Model
;; ---------------------------------------------------------------------------

(defrecord S3Register [value]
  Model
  (step [this op]
    (case (:f op)
      :read
      (if (= (:value op) value)
        this
        (model/inconsistent
         (str "stale read: expected " (pr-str value) ", got " (pr-str (:value op)))))

      :write
      (S3Register. (:value op))

      :delete
      (S3Register. nil)

      :cas
      (let [[expected replacement] (:value op)]
        (if (= value expected)
          (S3Register. replacement)
          (model/inconsistent
           (str "CAS mismatch: register holds " (pr-str value)
                ", expected " (pr-str expected)))))

      this)))

(defn s3-register-model
  "Initial empty S3 object register (`nil` represents 404 NotFound)."
  []
  (S3Register. nil))

(defn linearizable-register-checker
  "Knossos linearizability checker over the S3 register + CAS + delete model."
  []
  (checker/linearizable {:model (s3-register-model)
                         :algorithm :linear}))

;; ---------------------------------------------------------------------------
;; 2. Elle List-Append Consistency Checker
;; ---------------------------------------------------------------------------

(defn elle-append-checker
  "Wraps `elle.list-append/check` targeting strict serializability."
  ([] (elle-append-checker {}))
  ([elle-opts]
   (reify checker/Checker
     (check [_ _test history _opts]
       (elle-list/check
        (merge {:consistency-models [:strict-serializable]} elle-opts)
        history)))))

;; ---------------------------------------------------------------------------
;; 3. S3 Bucket Versioning Durability & Non-Resurrection Checker
;; ---------------------------------------------------------------------------

(defn- blank-version-id?
  [vid]
  (or (nil? vid) (= "" vid) (= "null" vid)))

(defn versioned-checker
  "Verifies S3 versioning guarantees across a Jepsen history:
    1. Every acknowledged PUT or delete marker has a unique, non-empty VersionId.
    2. Reading by an acknowledged VersionId never returns :not-found unless that
       exact [key VersionId] was explicitly deleted. A delete marker hides the
       current object and does not destroy older versions.
    3. Reading by VersionId always matches the exact SHA-256 recorded on PUT.
    4. Final ListObjectVersions includes every acknowledged version and delete marker.
    5. A final GetObject (no VersionId) agrees with the last completed current state:
       a delete marker yields NotFound, and a later acknowledged PUT is readable."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [ok-ops (filter #(= :ok (:type %)) history)
            puts (filter #(= :put-version (:f %)) ok-ops)
            markers (filter #(= :delete-marker (:f %)) ok-ops)
            del-versions (->> ok-ops
                              (filter #(= :delete-version (:f %)))
                              (keep (fn [op]
                                      (let [{:keys [key version-id]} (:value op)]
                                        (when (and key version-id) [key version-id]))))
                              set)
            known-versions (reduce (fn [acc op]
                                     (let [{:keys [key version-id sha256]} (:value op)]
                                       (assoc acc [key version-id] sha256)))
                                   {}
                                   puts)
            known-markers (reduce (fn [acc op]
                                    (let [{:keys [key version-id]} (:value op)]
                                      (conj acc [key version-id])))
                                  #{}
                                  markers)
            vid-uses (reduce (fn [acc [vid entry]]
                               (update acc vid (fnil conj []) entry))
                             {}
                             (concat (map (fn [op]
                                            (let [{:keys [key version-id sha256]} (:value op)]
                                              [version-id {:kind :version :key key :sha256 sha256}]))
                                          puts)
                                     (map (fn [op]
                                            (let [{:keys [key version-id]} (:value op)]
                                              [version-id {:kind :delete-marker :key key}]))
                                          markers)))
            duplicate-vid-errors
            (for [[vid entries] vid-uses
                  :when (and (not (blank-version-id? vid)) (> (count entries) 1))]
              {:type :duplicate-version-id :version-id vid :uses entries})

            missing-vid-errors
            (concat
             (for [op puts
                   :let [vid (get-in op [:value :version-id])]
                   :when (blank-version-id? vid)]
               {:type :missing-version-id :key (get-in op [:value :key])})
             (for [op markers
                   :let [{:keys [key version-id delete-marker?]} (:value op)]
                   :when (or (blank-version-id? version-id) (not delete-marker?))]
               {:type :delete-did-not-create-marker
                :key key :version-id version-id :delete-marker? delete-marker?}))

            get-version-errors
            (for [op (filter #(= :get-version (:f %)) ok-ops)
                  :let [{:keys [key version-id sha256 found?]} (:value op)
                        expected-sha (get known-versions [key version-id])
                        deleted? (contains? del-versions [key version-id])]
                  :when expected-sha
                  :let [err (cond
                              (and (not found?) (not deleted?))
                              {:type :lost-acknowledged-version
                               :key key :version-id version-id}

                              (and found? (not= sha256 expected-sha))
                              {:type :version-payload-corruption
                               :key key :version-id version-id
                               :expected expected-sha :actual sha256})]]
              err)

            current (reduce (fn [acc op]
                              (let [k (get-in op [:value :key])]
                                (cond
                                  (and (= :ok (:type op)) (= :put-version (:f op)))
                                  (assoc acc k {:state :version
                                                :sha256 (get-in op [:value :sha256])
                                                :version-id (get-in op [:value :version-id])})

                                  (and (= :ok (:type op)) (= :delete-marker (:f op)))
                                  (assoc acc k {:state :delete-marker
                                                :version-id (get-in op [:value :version-id])})

                                  (and (= :info (:type op))
                                       (#{:put-version :delete-marker} (:f op)))
                                  (assoc acc k {:state :indeterminate})

                                  :else acc)))
                            {}
                            history)

            current-errors
            (for [op ok-ops
                  :when (= :final-get-current (:f op))
                  :let [{:keys [key found? sha256]} (:value op)
                        st (get current key)
                        err (case (:state st)
                              :delete-marker
                              (when found?
                                {:type :visible-after-delete-marker :key key :sha256 sha256})

                              :version
                              (cond
                                (not found?)
                                {:type :lost-current-version :key key
                                 :version-id (:version-id st)}

                                (not= sha256 (:sha256 st))
                                {:type :current-payload-corruption :key key
                                 :expected (:sha256 st) :actual sha256})

                              nil
                              (when found?
                                {:type :phantom-current-object :key key :sha256 sha256})

                              nil)]]
              err)

            final-lists (filter #(= :final-list-versions (:f %)) ok-ops)
            final-list-errors
            (for [op final-lists
                  :let [listed-versions (set (map (juxt :key :version-id)
                                                  (get-in op [:value :versions])))
                        listed-markers (set (map (juxt :key :version-id)
                                                 (get-in op [:value :delete-markers])))]
                  err (concat
                       (for [[[k vid] _] known-versions
                             :when (and (not (contains? del-versions [k vid]))
                                        (not (contains? listed-versions [k vid])))]
                         {:type :version-missing-from-final-listing
                          :key k :version-id vid})
                       (for [[k vid] known-markers
                             :when (and (not (blank-version-id? vid))
                                        (not (contains? listed-markers [k vid])))]
                         {:type :delete-marker-missing-from-final-listing
                          :key k :version-id vid}))]
              err)

            errors (vec (concat duplicate-vid-errors
                                missing-vid-errors
                                (remove nil? get-version-errors)
                                (remove nil? current-errors)
                                final-list-errors))
            needs-listing? (or (seq known-versions) (seq known-markers))]
        {:valid? (cond
                   (seq errors) false
                   (and needs-listing? (empty? final-lists)) :unknown
                   :else true)
         :acknowledged-versions (count known-versions)
         :acknowledged-delete-markers (count known-markers)
         :errors errors}))))

(defn conditional-write-checker
  "Fails the run when a conditional write is rejected as NotImplemented.

  Knossos and Elle ignore :fail operations. A server (or client) that rejects
  every If-Match / If-None-Match request would otherwise produce a vacuous pass
  on unconditional reads and writes."
  [f]
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [unsupported
            (for [op history
                  :when (and (= f (:f op)) (= :fail (:type op)))
                  :let [err (:error op)]
                  :when (and (sequential? err)
                             (or (= 501 (second err))
                                 (= "NotImplemented" (nth err 2 nil))))]
              {:index (:index op)
               :f (:f op)
               :error err})]
        {:valid? (empty? unsupported)
         :unsupported-conditional-writes (count unsupported)
         :errors (vec (take 8 unsupported))}))))

;; ---------------------------------------------------------------------------
;; 4. Multipart Upload All-or-Nothing Atomicity Checker
;; ---------------------------------------------------------------------------

(defn- multipart-operations
  "Return successful reads and possible writes with their invocation indexes."
  [history]
  (let [[completed open]
        (reduce (fn [[completed open] {:keys [f process type index] :as op}]
                  (if-not (#{:complete-multipart :get-object :final-get} f)
                    [completed open]
                    (case type
                      :invoke [completed (assoc open process (assoc op :start index))]
                      (:ok :info :fail)
                      [(cond-> completed
                         (or (= :ok type) (and (= :info type) (= :complete-multipart f)))
                         (conj (assoc op :start (get-in open [process :start] index))))
                       (dissoc open process)])))
                [[] {}]
                history)]
    (into completed (filter #(= :complete-multipart (:f %))) (vals open))))

(defn multipart-checker
  "Verifies that reads expose only complete assemblies submitted for the same
  key. After healing, every key with an acknowledged complete must be read and
  show an assembly that could be current under the real-time order of writes."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [ops (multipart-operations history)
            completes (group-by :key (for [{:keys [f type index start value]} ops
                                            :when (= :complete-multipart f)]
                                        {:key (:key value)
                                         :sha256 (:expected-sha256 value)
                                         :start start
                                         :ack (when (= :ok type) index)}))
            by-assembly (into {} (for [[key uploads] completes]
                                   [key (group-by :sha256 uploads)]))
            ;; Completions can arrive in a different order from the writes at
            ;; the server. An acknowledged write rules out earlier assemblies
            ;; only when it started after those writes finished.
            ack-bounds (into {} (for [[key uploads] completes
                                      :let [acked (filter :ack uploads)]
                                      :when (seq acked)]
                                  [key {:first-ack (apply min (map :ack acked))
                                        :last-start (apply max (map :start acked))}]))
            reads (filter #(#{:get-object :final-get} (:f %)) ops)
            final-gets (filter #(= :final-get (:f %)) reads)
            final-keys (set (map #(get-in % [:value :key]) final-gets))
            missing-keys (vec (sort (remove final-keys (keys ack-bounds))))
            errors
            (vec (for [{:keys [f index value start]} reads
                       :let [{:keys [key found? sha256]} value
                             {:keys [first-ack last-start]} (get ack-bounds key)
                             cutoff (when (= :final-get f) last-start)
                             visible? (fn [{:keys [start ack]}]
                                        (and (< start index)
                                             (or (nil? cutoff) (nil? ack) (>= ack cutoff))))
                             error (cond
                                     (and first-ack (< first-ack start) (not found?))
                                     :lost-completed-multipart

                                     (and found? (not-any? visible? (get-in by-assembly [key sha256])))
                                     (if cutoff
                                       :stale-or-corrupt-final-multipart
                                       :partial-or-corrupt-multipart-read))]
                       :when error]
                   {:type error :key key :index index
                    :expected-sha256s (into #{} (comp (filter visible?) (map :sha256))
                                            (get completes key))
                    :actual-sha256 sha256}))]
        {:valid? (cond
                   (seq errors) false
                   (or (empty? ack-bounds) (seq missing-keys)) :unknown
                   :else true)
         :completed-keys (count ack-bounds)
         :final-gets (count final-gets)
         :missing-final-gets missing-keys
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 5. Read-After-Write & ListObjectsV2 Convergence Checker
;; ---------------------------------------------------------------------------

(defn list-consistency-checker
  "Verifies S3 ListObjectsV2 completeness and zero-phantom invariants:
    1. Every acknowledged live key (PUT and never deleted) MUST appear in the
       final post-heal ListObjectsV2 result.
    2. Every key returned in ListObjectsV2 MUST have been written (no phantom keys).
    3. Every key acknowledged deleted (and never re-PUT) MUST NOT appear in the
       final post-heal ListObjectsV2 result."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [;; :ok/:info puts may be visible. An unmatched :invoke may have
            ;; committed. A :fail put is a definite rejection and must not
            ;; become visible unless a different attempt of that key did.
            write-fate (reduce (fn [acc op]
                                 (if-not (and (= :put (:f op))
                                              (some? (get-in op [:value :key])))
                                   acc
                                   (let [k (get-in op [:value :key])
                                         st (get acc k :absent)]
                                     (case (:type op)
                                       :invoke (if (= st :possible) acc (assoc acc k :inflight))
                                       (:ok :info) (assoc acc k :possible)
                                       :fail (if (= st :possible) acc (assoc acc k :rejected))
                                       acc))))
                               {}
                               history)
            ;; Track final acknowledged state per key in completion order.
            ;; Jepsen histories record a completion as its own event when the
            ;; operation finishes, so :ok/:info events are already in real-time order.
            key-state (reduce (fn [acc op]
                                (let [k (get-in op [:value :key])]
                                  (cond
                                    (and (= :ok (:type op)) (= :put (:f op)))
                                    (assoc acc k {:state :live :sha256 (get-in op [:value :sha256])})

                                    (and (= :ok (:type op)) (= :delete (:f op)))
                                    (assoc acc k {:state :deleted})

                                    (and (= :info (:type op)) (#{:put :delete} (:f op)))
                                    (assoc acc k {:state :indeterminate})

                                    :else acc)))
                              {}
                              history)
            final-lists (filter #(and (= :ok (:type %)) (= :final-list (:f %))) history)
            errors
            (vec
             (for [op final-lists
                   :let [listed-keys (set (map :key (:value op)))
                         missing (for [[k {:keys [state]}] key-state
                                       :when (and (= state :live)
                                                  (not (contains? listed-keys k)))]
                                   {:type :missing-committed-key :key k})
                         resurrected (for [[k {:keys [state]}] key-state
                                           :when (and (= state :deleted)
                                                      (contains? listed-keys k))]
                                       {:type :deleted-key-resurrected :key k})
                         phantoms (for [k listed-keys
                                        :let [fate (get write-fate k :absent)]
                                        :when (not (#{:possible :inflight} fate))]
                                    {:type (if (= fate :rejected)
                                             :definite-failed-write-visible
                                             :phantom-key)
                                     :key k})]
                   err (concat missing resurrected phantoms)]
               err))]
        {:valid? (and (seq final-lists) (empty? errors))
         :tracked-keys (count key-state)
         :final-list-checks (count final-lists)
         :errors errors}))))
