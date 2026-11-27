(ns rustfs.jepsen.checker
  "Consistency, linearizability, serializability, versioning, multipart
  atomicity, and listing convergence checkers for RustFS Jepsen runs.

  Reuses official Jepsen verification engines:
    - Knossos (`jepsen.checker/linearizable`) for single-object S3 register + CAS
    - Elle (`elle.list-append/check`) for transactional anomaly detection
      (G0, G1a, G1b, G1c, G-single, G2-item)"
  (:require [elle.list-append :as elle-list]
            [jepsen.checker :as checker]
            [knossos.model :as model]))

;; ---------------------------------------------------------------------------
;; 1. Knossos Linearizable S3 Register + Delete + Conditional CAS Model
;; ---------------------------------------------------------------------------

(defrecord S3Register [value]
  model/Model
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

(defn versioned-checker
  "Verifies S3 versioning guarantees across a Jepsen history:
    1. Every acknowledged PUT produced a non-empty VersionId.
    2. Reading by an acknowledged VersionId never returns :not-found unless that
       exact VersionId was explicitly deleted.
    3. Reading by VersionId always matches the exact SHA-256 recorded on PUT.
    4. Final ListObjectVersions includes every acknowledged non-deleted VersionId."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [ok-ops (filter #(= :ok (:type %)) history)
            puts (filter #(= :put-version (:f %)) ok-ops)
            del-versions (->> ok-ops
                              (filter #(= :delete-version (:f %)))
                              (map #(get-in % [:value :version-id]))
                              set)
            known-versions (reduce (fn [acc op]
                                     (let [{:keys [key version-id sha256]} (:value op)]
                                       (assoc acc [key version-id] sha256)))
                                   {}
                                   puts)
            missing-vid-errors
            (for [op puts
                  :let [vid (get-in op [:value :version-id])]
                  :when (or (nil? vid) (= "" vid) (= "null" vid))]
              {:type :missing-version-id :op op})

            get-version-errors
            (for [op (filter #(= :get-version (:f %)) ok-ops)
                  :let [{:keys [key version-id sha256 found?]} (:value op)
                        expected-sha (get known-versions [key version-id])
                        deleted? (contains? del-versions version-id)]
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

            final-lists (filter #(= :final-list-versions (:f %)) ok-ops)
            final-list-errors
            (for [op final-lists
                  :let [listed (set (map (juxt :key :version-id) (get-in op [:value :versions])))]
                  [[k vid] _] known-versions
                  :when (and (not (contains? del-versions vid))
                             (not (contains? listed [k vid])))]
              {:type :version-missing-from-final-listing
               :key k :version-id vid})

            errors (vec (concat missing-vid-errors
                                (remove nil? get-version-errors)
                                final-list-errors))]
        {:valid? (empty? errors)
         :acknowledged-versions (count known-versions)
         :errors errors}))))

;; ---------------------------------------------------------------------------
;; 4. Multipart Upload All-or-Nothing Atomicity Checker
;; ---------------------------------------------------------------------------

(defn multipart-checker
  "Verifies multipart upload atomicity and bit-exact payload integrity:
    1. Any read of a key MUST return either :not-found or a payload whose SHA-256
       matches a submitted/completed full multipart assembly (never partial parts
       or corrupted bytes).
    2. After a final read post-heal, every acknowledged completed upload is
       readable with matching SHA-256."
  []
  (reify checker/Checker
    (check [_ _test history _opts]
      (let [valid-hashes (->> history
                              (filter #(and (= :complete-multipart (:f %))
                                            (#{:invoke :ok :info} (:type %))))
                              (keep #(get-in % [:value :expected-sha256]))
                              set)
            last-acked (reduce (fn [acc op]
                                 (if (and (= :ok (:type op))
                                          (= :complete-multipart (:f op)))
                                   (assoc acc (get-in op [:value :key])
                                          (get-in op [:value :expected-sha256]))
                                   acc))
                               {}
                               history)
            read-errors
            (for [op history
                  :when (and (= :ok (:type op))
                             (#{:get-object :final-get} (:f op)))
                  :let [{:keys [key found? sha256]} (:value op)]
                  :when (and found? (not (contains? valid-hashes sha256)))]
              {:type :partial-or-corrupt-multipart-read
               :key key :actual-sha256 sha256})

            final-errors
            (for [op history
                  :when (and (= :ok (:type op))
                             (= :final-get (:f op)))
                  :let [{:keys [key found? sha256]} (:value op)
                        acked-sha (get last-acked key)]
                  :when (and acked-sha (not found?))]
              {:type :lost-completed-multipart
               :key key :expected-sha256 acked-sha})

            errors (vec (concat read-errors final-errors))]
        {:valid? (empty? errors)
         :completed-keys (count last-acked)
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
      (let [ever-written (->> history
                              (filter #(and (= :put (:f %)) (#{:invoke :ok :info} (:type %))))
                              (keep #(get-in % [:value :key]))
                              set)
            ;; Track final acknowledged state per key in history order.
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
                                        :when (not (contains? ever-written k))]
                                    {:type :phantom-key :key k})]
                   err (concat missing resurrected phantoms)]
               err))]
        {:valid? (and (seq final-lists) (empty? errors))
         :tracked-keys (count key-state)
         :final-list-checks (count final-lists)
         :errors errors}))))
