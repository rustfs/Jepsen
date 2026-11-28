(ns rustfs.jepsen.checker-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.checker :as checker]
            [jepsen.history :as h]
            [rustfs.jepsen.checker :as rcheck]))

(defn- mk-history
  [ops]
  (h/history
   (map-indexed (fn [idx op]
                  (merge {:index idx :time (* idx 1000000)} op))
                ops)))

(deftest nemesis-exception-makes-the-fault-campaign-inconclusive
  (let [chk (rcheck/nemesis-exceptions-checker)
        fault {:process :nemesis :type :info :f :fail-drive}
        error {:cause "cannot change drive mode"}
        failed-heal (assoc fault :f :restore-drive :exception error :error (:cause error))]
    (is (true? (:valid? (checker/check chk nil (mk-history []) {}))))
    (is (true? (:valid? (checker/check chk nil (mk-history [fault]) {}))))
    (is (true? (:valid? (checker/check chk nil (mk-history [(assoc fault :process 0 :exception error)]) {}))))
    (let [result (checker/check chk nil (mk-history [fault failed-heal]) {})]
      (is (= :unknown (:valid? result)))
      (is (= 1 (:error-count result)))
      (is (= [{:index 1 :f :restore-drive :error "cannot change drive mode"}]
             (:errors result)))
      (is (false? (checker/merge-valid [false (:valid? result)]))))))

(deftest knossos-s3-register-linearizability-test
  (let [chk (rcheck/linearizable-register-checker)]
    (testing "accepts a valid linearizable PUT -> GET -> CAS -> DELETE history"
      (let [hist (mk-history
                  [{:process 0 :type :invoke :f :write :value 1}
                   {:process 0 :type :ok     :f :write :value 1}
                   {:process 1 :type :invoke :f :read  :value nil}
                   {:process 1 :type :ok     :f :read  :value 1}
                   {:process 0 :type :invoke :f :cas   :value [1 2]}
                   {:process 0 :type :ok     :f :cas   :value [1 2]}
                   {:process 1 :type :invoke :f :read  :value nil}
                   {:process 1 :type :ok     :f :read  :value 2}
                   {:process 0 :type :invoke :f :delete :value nil}
                   {:process 0 :type :ok     :f :delete :value nil}
                   {:process 1 :type :invoke :f :read  :value nil}
                   {:process 1 :type :ok     :f :read  :value nil}])]
        (is (true? (:valid? (checker/check chk nil hist {}))))))

    (testing "rejects a stale read after an acknowledged overwrite"
      (let [hist (mk-history
                  [{:process 0 :type :invoke :f :write :value 1}
                   {:process 0 :type :ok     :f :write :value 1}
                   {:process 0 :type :invoke :f :write :value 2}
                   {:process 0 :type :ok     :f :write :value 2}
                   {:process 1 :type :invoke :f :read  :value nil}
                   {:process 1 :type :ok     :f :read  :value 1}])]
        (is (false? (:valid? (checker/check chk nil hist {}))))))

    (testing "rejects concurrent double-success CAS from same expected value"
      (let [hist (mk-history
                  [{:process 0 :type :invoke :f :write :value 1}
                   {:process 0 :type :ok     :f :write :value 1}
                   {:process 1 :type :invoke :f :cas   :value [1 2]}
                   {:process 2 :type :invoke :f :cas   :value [1 3]}
                   {:process 1 :type :ok     :f :cas   :value [1 2]}
                   {:process 2 :type :ok     :f :cas   :value [1 3]}])]
        (is (false? (:valid? (checker/check chk nil hist {}))))))))

(deftest elle-list-append-checker-test
  (let [chk (rcheck/elle-append-checker)]
    (testing "accepts a valid strictly serializable append history"
      (let [hist (mk-history
                  [{:process 0 :type :invoke :f :txn :value [[:append 0 1]]}
                   {:process 0 :type :ok     :f :txn :value [[:append 0 1]]}
                   {:process 1 :type :invoke :f :txn :value [[:append 0 2]]}
                   {:process 1 :type :ok     :f :txn :value [[:append 0 2]]}
                   {:process 2 :type :invoke :f :txn :value [[:r 0 nil]]}
                   {:process 2 :type :ok     :f :txn :value [[:r 0 [1 2]]]}])]
        (is (true? (:valid? (checker/check chk nil hist {}))))))

    (testing "rejects an aborted read (G1a) anomaly"
      (let [hist (mk-history
                  [{:process 0 :type :invoke :f :txn :value [[:append 0 1]]}
                   {:process 0 :type :fail   :f :txn :value [[:append 0 1]]}
                   {:process 1 :type :invoke :f :txn :value [[:r 0 nil]]}
                   {:process 1 :type :ok     :f :txn :value [[:r 0 [1]]]}])]
        (is (false? (:valid? (checker/check chk nil hist {}))))))))

(deftest versioned-multipart-and-list-checkers-test
  (testing "versioned checker accepts durable versions and rejects lost version"
    (let [chk (rcheck/versioned-checker)
          good (mk-history
                [{:process 0 :type :ok :f :put-version :value {:key "ver-0" :version-id "v1" :sha256 "aaa"}}
                 {:process 1 :type :ok :f :get-version :value {:key "ver-0" :version-id "v1" :found? true :sha256 "aaa"}}
                 {:process 0 :type :ok :f :final-list-versions :value {:versions [{:key "ver-0" :version-id "v1"}]}}])
          bad (mk-history
               [{:process 0 :type :ok :f :put-version :value {:key "ver-0" :version-id "v1" :sha256 "aaa"}}
                {:process 1 :type :ok :f :get-version :value {:key "ver-0" :version-id "v1" :found? false}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {}))))))

  (testing "versioned checker rejects reused version ids and a current object left behind a delete marker"
    (let [chk (rcheck/versioned-checker)
          dup (mk-history
               [{:process 0 :type :ok :f :put-version :value {:key "ver-0" :version-id "v1" :sha256 "aaa"}}
                {:process 0 :type :ok :f :put-version :value {:key "ver-0" :version-id "v1" :sha256 "bbb"}}
                {:process 0 :type :ok :f :final-list-versions
                 :value {:versions [{:key "ver-0" :version-id "v1"}] :delete-markers []}}])
          marker (mk-history
                  [{:process 0 :type :ok :f :put-version :value {:key "ver-0" :version-id "v1" :sha256 "aaa"}}
                   {:process 0 :type :ok :f :delete-marker :value {:key "ver-0" :version-id "dm1" :delete-marker? true}}
                   {:process 1 :type :ok :f :get-version :value {:key "ver-0" :version-id "v1" :found? true :sha256 "aaa"}}
                   {:process 1 :type :ok :f :final-get-current :value {:key "ver-0" :found? true :sha256 "aaa"}}
                   {:process 0 :type :ok :f :final-list-versions
                    :value {:versions [{:key "ver-0" :version-id "v1"}]
                            :delete-markers [{:key "ver-0" :version-id "dm1"}]}}])
          hidden (mk-history
                  [{:process 0 :type :ok :f :put-version :value {:key "ver-0" :version-id "v1" :sha256 "aaa"}}
                   {:process 0 :type :ok :f :delete-marker :value {:key "ver-0" :version-id "dm1" :delete-marker? true}}
                   {:process 1 :type :ok :f :final-get-current :value {:key "ver-0" :found? false}}
                   {:process 0 :type :ok :f :final-list-versions
                    :value {:versions [{:key "ver-0" :version-id "v1"}]
                            :delete-markers [{:key "ver-0" :version-id "dm1"}]}}])]
      (is (false? (:valid? (checker/check chk nil dup {}))))
      (is (false? (:valid? (checker/check chk nil marker {}))))
      (is (true? (:valid? (checker/check chk nil hidden {}))))))

  (testing "an indeterminate delete marker may hide the current object without failing the run"
    (let [chk (rcheck/versioned-checker)
          hist (mk-history
                [{:process 0 :type :ok :f :put-version :value {:key "ver-0" :version-id "v1" :sha256 "aaa"}}
                 {:process 1 :type :info :f :delete-marker :value {:key "ver-0"}}
                 {:process 1 :type :ok :f :final-get-current :value {:key "ver-0" :found? false}}
                 {:process 0 :type :ok :f :final-list-versions
                  :value {:versions [{:key "ver-0" :version-id "v1"}] :delete-markers []}}])]
      (is (true? (:valid? (checker/check chk nil hist {}))))))

  (testing "multipart checker accepts full assembly and rejects partial/corrupt bytes"
    (let [chk (rcheck/multipart-checker)
          good (mk-history
                [{:process 0 :type :invoke :f :complete-multipart :value {:key "mp-0" :expected-sha256 "fullhash"}}
                 {:process 0 :type :ok     :f :complete-multipart :value {:key "mp-0" :expected-sha256 "fullhash"}}
                 {:process 1 :type :ok     :f :final-get          :value {:key "mp-0" :found? true :sha256 "fullhash"}}])
          bad (mk-history
               [{:process 0 :type :invoke :f :complete-multipart :value {:key "mp-0" :expected-sha256 "fullhash"}}
                {:process 1 :type :ok     :f :get-object         :value {:key "mp-0" :found? true :sha256 "partial-garbage"}}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil bad {}))))))

  (testing "multipart checker rejects a definitely failed complete that becomes visible, and a final read of an older assembly"
    (let [chk (rcheck/multipart-checker)
          failed (mk-history
                  [{:process 0 :type :invoke :f :complete-multipart :value {:key "mp-0" :expected-sha256 "fullhash"}}
                   {:process 0 :type :fail   :f :complete-multipart :value {:key "mp-0" :expected-sha256 "fullhash"} :error [:definite-fail 400 "EntityTooSmall" "too small"]}
                   {:process 1 :type :ok     :f :get-object         :value {:key "mp-0" :found? true :sha256 "fullhash"}}])
          stale (mk-history
                 [{:process 0 :type :ok :f :complete-multipart :value {:key "mp-0" :expected-sha256 "old"}}
                  {:process 0 :type :ok :f :complete-multipart :value {:key "mp-0" :expected-sha256 "new"}}
                  {:process 1 :type :ok :f :final-get          :value {:key "mp-0" :found? true :sha256 "old"}}])]
      (is (false? (:valid? (checker/check chk nil failed {}))))
      (is (false? (:valid? (checker/check chk nil stale {}))))))

  (testing "list-consistency checker detects missing committed keys and phantom keys"
    (let [chk (rcheck/list-consistency-checker)
          good (mk-history
                [{:process 0 :type :ok :f :put :value {:key "list-k-1" :sha256 "h1"}}
                 {:process 0 :type :ok :f :put :value {:key "list-k-2" :sha256 "h2"}}
                 {:process 0 :type :ok :f :delete :value {:key "list-k-2"}}
                 {:process 1 :type :ok :f :final-list :value [{:key "list-k-1"}]}])
          missing (mk-history
                   [{:process 0 :type :ok :f :put :value {:key "list-k-1" :sha256 "h1"}}
                    {:process 1 :type :ok :f :final-list :value []}])
          phantom (mk-history
                   [{:process 0 :type :ok :f :put :value {:key "list-k-1" :sha256 "h1"}}
                    {:process 1 :type :ok :f :final-list :value [{:key "list-k-1"} {:key "ghost"}]}])]
      (is (true? (:valid? (checker/check chk nil good {}))))
      (is (false? (:valid? (checker/check chk nil missing {}))))
      (is (false? (:valid? (checker/check chk nil phantom {}))))))

  (testing "list-consistency checker rejects a definitely failed put that is listed"
    (let [chk (rcheck/list-consistency-checker)
          failed-visible (mk-history
                          [{:process 0 :type :invoke :f :put :value {:key "list-k-9" :sha256 "h"}}
                           {:process 0 :type :fail   :f :put :value {:key "list-k-9" :sha256 "h"}
                            :error [:definite-fail 400 "InvalidArgument" "no"]}
                           {:process 1 :type :ok :f :final-list :value [{:key "list-k-9"}]}])]
      (is (false? (:valid? (checker/check chk nil failed-visible {}))))))

  (testing "conditional-write checker rejects NotImplemented instead of a vacuous pass"
    (let [chk (rcheck/conditional-write-checker :cas)
          unsupported (mk-history
                       [{:process 0 :type :invoke :f :cas :value [nil 1]}
                        {:process 0 :type :fail :f :cas :value [nil 1]
                         :error [:definite-fail 501 "NotImplemented" "conditional writes"]}])
          conflict (mk-history
                    [{:process 0 :type :invoke :f :cas :value [1 2]}
                     {:process 0 :type :fail :f :cas :value [1 2]
                      :error [:precondition 412 "PreconditionFailed" "mismatch"]}])]
      (is (false? (:valid? (checker/check chk nil unsupported {}))))
      (is (true? (:valid? (checker/check chk nil conflict {})))))))

(deftest multipart-checker-attaches-assemblies-to-their-key-and-invocation
  (let [chk (rcheck/multipart-checker)
        complete {:process 0 :f :complete-multipart
                  :value {:key "mp-0" :expected-sha256 "full"}}
        read {:process 1 :type :ok :f :get-object
              :value {:key "mp-0" :found? true :sha256 "full"}}]
    (testing "a full assembly for a different key is still corruption"
      (let [hist (mk-history [(assoc complete :type :invoke)
                              (assoc complete :type :ok)
                              (assoc-in read [:value :key] "mp-1")])]
        (is (false? (:valid? (checker/check chk nil hist {}))))))
    (testing "an assembly invoked after the read cannot explain it"
      (let [hist (mk-history [read (assoc complete :type :invoke) (assoc complete :type :ok)])]
        (is (false? (:valid? (checker/check chk nil hist {}))))))))

(deftest multipart-checker-requires-post-heal-evidence-for-every-acknowledged-key
  (let [chk (rcheck/multipart-checker)
        complete {:process 0 :f :complete-multipart
                  :value {:key "mp-0" :expected-sha256 "full"}}
        final-get {:process 1 :type :ok :f :final-get
                   :value {:key "mp-0" :found? true :sha256 "full"}}]
    (testing "a successful final GET of another key is insufficient"
      (let [hist (mk-history [(assoc complete :type :invoke)
                              (assoc complete :type :ok)
                              (assoc final-get :value {:key "mp-1" :found? false})])
            result (checker/check chk nil hist {})]
        (is (= :unknown (:valid? result)))
        (is (= ["mp-0"] (:missing-final-gets result)))))
    (testing "the test is inconclusive when no complete succeeded"
      (let [hist (mk-history [(assoc complete :type :invoke)
                              (assoc complete :type :fail)
                              (assoc final-get :value {:key "mp-0" :found? false})])]
        (is (= :unknown (:valid? (checker/check chk nil hist {}))))))))

(deftest multipart-checker-allows-both-orders-of-overlapping-completes
  (let [chk (rcheck/multipart-checker)
        first-upload {:process 0 :f :complete-multipart
                      :value {:key "mp-0" :expected-sha256 "one"}}
        second-upload {:process 1 :f :complete-multipart
                       :value {:key "mp-0" :expected-sha256 "two"}}
        final-get {:process 2 :type :ok :f :final-get
                   :value {:key "mp-0" :found? true :sha256 "two"}}
        hist (mk-history [(assoc first-upload :type :invoke)
                          (assoc second-upload :type :invoke)
                          (assoc second-upload :type :ok)
                          (assoc first-upload :type :ok)
                          final-get])]
    (is (true? (:valid? (checker/check chk nil hist {}))))
    (is (true? (:valid? (checker/check chk nil
                                      (mk-history (conj (vec (butlast hist))
                                                        (assoc-in final-get [:value :sha256] "one")))
                                      {}))))))

(deftest multipart-checker-rejects-only-missing-reads-that-start-after-acknowledgment
  (let [chk (rcheck/multipart-checker)
        upload {:process 0 :f :complete-multipart
                :value {:key "mp-0" :expected-sha256 "full"}}
        read {:process 1 :f :get-object :value {:key "mp-0" :found? false}}
        final-get {:process 1 :type :ok :f :final-get
                   :value {:key "mp-0" :found? true :sha256 "full"}}
        events [(assoc upload :type :invoke) (assoc upload :type :ok)
                (assoc read :type :invoke) (assoc read :type :ok) final-get]
        lost (mk-history events)
        overlapping (mk-history (mapv events [0 2 1 3 4]))]
    (is (= :lost-completed-multipart
           (-> (checker/check chk nil lost {}) :errors first :type)))
    (is (true? (:valid? (checker/check chk nil overlapping {}))))))

(deftest list-checker-requires-final-evidence-and-unique-listed-keys
  (let [chk (rcheck/list-consistency-checker)
        put {:process 0 :type :ok :f :put :value {:key "list-k-1" :sha256 "h1"}}
        list-op {:process 1 :f :final-list}]
    (is (= :unknown (:valid? (checker/check chk nil (mk-history [put (assoc list-op :type :fail)]) {}))))
    (let [hist (mk-history [put (assoc list-op :type :ok :value [{:key "list-k-1"} {:key "list-k-1"}])])
          result (checker/check chk nil hist {})]
      (is (false? (:valid? result)))
      (is (= [{:type :duplicate-listed-key :key "list-k-1" :count 2}] (:errors result))))))
