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
      (is (false? (:valid? (checker/check chk nil phantom {})))))))
