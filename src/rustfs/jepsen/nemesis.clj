(ns rustfs.jepsen.nemesis
  "Fault-injection nemesis composition for RustFS Jepsen runs.

  Composes the selected packages from `jepsen.nemesis.combined`:
    - :partition -> network splits (halves, majorities-ring, random node)
    - :kill      -> SIGKILL + restart of rustfs server processes
    - :pause     -> SIGSTOP / SIGCONT process stalls (GC/IO pause simulation)
    - :packet    -> tc netem packet delay, loss, reorder, and corruption
    - :clock     -> clock skew bumps (VM / bare-metal nodes only)

  Plus a custom RustFS-specific `:drive` nemesis that takes a single drive
  directory unreadable (`chmod 000`) and later restores its original mode to
  verify erasure-coded degraded operation and online recovery."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info]]
            [jepsen.control :as c]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [rustfs.jepsen.db :as rdb]
            [rustfs.jepsen.topology :as topo]))

(def supported-faults
  #{:partition :kill :pause :packet :drive :clock})

(defn- restore-drives!
  [test faulted]
  (let [targets @faulted]
    (doseq [[[node dir :as target] mode] targets]
      (c/on-nodes test [node] (fn [_ _] (c/su (c/exec :chmod mode dir))))
      (swap! faulted dissoc target))
    (vec (keys targets))))

(defn drive-fault-nemesis
  "Denies the RustFS service user access to one drive directory."
  []
  (let [faulted (atom {})]
    (reify
      nemesis/Reflection
      (fs [_] #{:fail-drive :restore-drive})

      nemesis/Nemesis
      (setup! [this _test] this)

      (invoke! [_ test op]
        (case (:f op)
          :fail-drive
          (let [node (rand-nth (:nodes test))
                drives (:drives-per-node (get topo/topologies (:topology test)))
                idx (inc (rand-int drives))
                dir (topo/data-dir idx)]
            (c/on-nodes test [node]
                        (fn [_ _]
                          (c/su
                           (let [mode (c/exec :stat :-c "%a" dir)]
                             ;; Track the original mode even if chmod succeeds
                             ;; remotely and its SSH response is lost.
                             (swap! faulted update [node dir] #(or % mode))
                             (c/exec :chmod "0000" dir)))
                          (c/sudo rdb/daemon-user (c/exec :test :! :-x dir))))
            (info "Faulted drive" node dir)
            (assoc op :value {:node node :dir dir}))

          :restore-drive
          (let [targets (restore-drives! test faulted)]
            (info "Restored faulted drives" targets)
            (assoc op :value targets))))

      (teardown! [_ test]
        (restore-drives! test faulted)))))

(defn drive-fault-package
  [{:keys [faults interval]}]
  (when (contains? faults :drive)
    {:nemesis (drive-fault-nemesis)
     :generator (->> (cycle [{:type :info :f :fail-drive}
                             {:type :info :f :restore-drive}])
                     (gen/stagger interval))
     :final-generator (gen/once {:type :info :f :restore-drive})
     :perf #{{:name "drive"
              :start #{:fail-drive}
              :stop #{:restore-drive}
              :color "#D97706"}}}))

(defn readiness-barrier-nemesis
  "Waits for every cluster node's S3 API to answer after all partitions/kills heal ([J-06])."
  []
  (reify
    nemesis/Reflection
    (fs [_] #{:wait-cluster-ready})

    nemesis/Nemesis
    (setup! [this _test] this)
    (invoke! [_ test op]
      (doseq [node (:nodes test)]
        (rdb/wait-ready! test node))
      (assoc op :value :all-nodes-ready))
    (teardown! [_ _test])))

(defn build-nemesis-package
  "Compose only requested nemeses so disabled faults have no lifecycle hooks."
  [opts]
  (let [faults (set (:faults opts))
        unknown (set/difference faults supported-faults)
        config {:db (:db opts)
                :faults faults
                :partition {:targets [:one :majority :majorities-ring]}
                :kill {:targets [:one]}
                :pause {:targets [:one]}
                :packet {:targets [:one]
                         :behaviors [{:delay {:time :100ms :jitter :40ms}}
                                     {:loss {:percent :15%}}]}
                :clock {:targets [:one]}
                :interval (:nemesis-interval opts 12)}]
    (when (seq unknown)
      (throw (ex-info "Unknown RustFS nemesis faults." {:faults unknown})))
    (nc/compose-packages
     (cond-> []
       (:partition faults) (conj (nc/partition-package config))
       (:packet faults) (conj (nc/packet-package config))
       (:drive faults) (conj (drive-fault-package config))
       (:clock faults) (conj (nc/clock-package config))
       (some faults [:kill :pause]) (conj (nc/db-package config))))))
