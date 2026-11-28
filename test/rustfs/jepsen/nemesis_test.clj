(ns rustfs.jepsen.nemesis-test
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.nemesis :as nemesis]
            [rustfs.jepsen.db :as rdb]
            [rustfs.jepsen.nemesis :as rnem]))

(deftest only-requested-faults-are-installed
  (let [opts {:db (rdb/db) :nodes ["n1" "n2" "n3"]}]
    (testing "none has no nemesis operations or cleanup"
      (let [pkg (rnem/build-nemesis-package (assoc opts :faults #{}))]
        (is (= #{} (nemesis/fs (:nemesis pkg))))
        (is (nil? (:generator pkg)))
        (is (nil? (:final-generator pkg)))))
    (testing "only enabled nemeses get composed"
      (let [pkg (rnem/build-nemesis-package (assoc opts :faults #{:drive :partition :packet}))]
        (is (= #{:fail-drive :restore-drive :start-partition :stop-partition
                 :start-packet :stop-packet}
               (set (nemesis/fs (:nemesis pkg)))))))
    (testing "misspelled faults are rejected"
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown RustFS nemesis faults"
                            (rnem/build-nemesis-package (assoc opts :faults #{:partiton})))))))

(deftest drive-fault-checks-daemon-access-and-restores-original-mode
  (let [test {:nodes ["n1"] :topology :3x2}
        dir "/var/lib/rustfs/data1"
        commands (atom [])
        drive (rnem/drive-fault-nemesis)]
    (with-redefs [rand-int (constantly 0)
                  c/on-nodes (fn [test [node] f] (f test node))
                  c/exec (fn [& args]
                           (swap! commands conj [c/*sudo* (vec args)])
                           (when (= :stat (first args)) "750"))]
      (is (= {:node "n1" :dir dir}
             (:value (nemesis/invoke! drive test {:f :fail-drive}))))
      (is (= [["n1" dir]]
             (:value (nemesis/invoke! drive test {:f :restore-drive}))))
      (nemesis/teardown! drive test)
      (is (= [["root" [:stat :-c "%a" dir]]
              ["root" [:chmod "0000" dir]]
              [rdb/daemon-user [:test :! :-x dir]]
              ["root" [:chmod "750" dir]]]
             @commands)))))

(deftest drive-mode-is-tracked-before-a-remote-change
  (let [test {:nodes ["n1"] :topology :3x2}
        dir "/var/lib/rustfs/data1"
        modes (atom [])
        drive (rnem/drive-fault-nemesis)]
    (with-redefs [rand-int (constantly 0)
                  c/on-nodes (fn [test [node] f] (f test node))
                  c/exec (fn [& [command mode]]
                           (case command
                             :stat "751"
                             :chmod (do (swap! modes conj mode)
                                        (when (= "0000" mode)
                                          (throw (ex-info "SSH response lost" {}))))))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"SSH response lost"
                            (nemesis/invoke! drive test {:f :fail-drive})))
      (is (= [["n1" dir]] (nemesis/teardown! drive test)))
      (is (= ["0000" "751"] @modes)))))

(deftest daemon-runs-as-drive-fault-probe-user
  (let [test {:nodes ["n1" "n2" "n3"] :topology :3x2 :s3-port 9000}
        launch (atom nil)]
    (with-redefs [rdb/write-credentials! (constantly nil)
                  rdb/data-device-ids! (constantly ["1" "2"])
                  cu/start-daemon! (fn [opts & _] (reset! launch [c/*sudo* opts]))]
      (c/su (rdb/start-node! test "n1"))
      (is (= rdb/daemon-user (first @launch)))
      (is (= rdb/pid-file (get-in @launch [1 :pidfile]))))))
