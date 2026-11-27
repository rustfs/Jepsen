(ns rustfs.jepsen.topology-test
  (:require [clojure.test :refer [deftest is testing]]
            [rustfs.jepsen.topology :as topo]))

(deftest official-quorum-math-test
  (testing "4x4 topology matches RustFS 16-drive EC:4 quorums"
    (let [s (:4x4 topo/topologies)]
      (is (= 4 (:nodes s)))
      (is (= 4 (:drives-per-node s)))
      (is (= 16 (:drives s)))
      (is (= 4 (:parity s)))
      (is (= 12 (:read-quorum s)))
      (is (= 12 (:write-quorum s)))
      (is (true? (:survives-one-node-loss? s)))))

  (testing "3x2 topology matches RustFS 6-drive EC:3 quorums (W=4 prevents split-brain)"
    (let [s (:3x2 topo/topologies)]
      (is (= 3 (:nodes s)))
      (is (= 2 (:drives-per-node s)))
      (is (= 6 (:drives s)))
      (is (= 3 (:parity s)))
      (is (= 3 (:read-quorum s)))
      (is (= 4 (:write-quorum s)))
      (is (true? (:survives-one-node-loss? s)))))

  (testing "4x2 topology matches RustFS 8-drive EC:4 quorums (W=5 prevents 2-2 split-brain)"
    (let [s (:4x2 topo/topologies)]
      (is (= 8 (:drives s)))
      (is (= 4 (:parity s)))
      (is (= 4 (:read-quorum s)))
      (is (= 5 (:write-quorum s))))))

(deftest volumes-ellipsis-expansion-test
  (is (= "http://n{1...4}:9000/var/lib/rustfs/data{1...4}"
         (topo/volumes-arg ["n1" "n2" "n3" "n4"] :4x4 9000)))
  (is (= "http://n{1...3}.jepsen.local:9000/var/lib/rustfs/data{1...2}"
         (topo/volumes-arg ["n1.jepsen.local" "n2.jepsen.local" "n3.jepsen.local"] :3x2 9000)))
  (is (= ["n1" "n2" "n3"]
         (topo/resolve-nodes topo/default-jepsen-nodes :3x2 "n%d")))
  (is (thrown? clojure.lang.ExceptionInfo
               (topo/volumes-arg ["n1" "n2" "n4" "n5"] :4x4 9000))))
