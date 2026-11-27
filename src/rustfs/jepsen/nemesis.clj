(ns rustfs.jepsen.nemesis
  "Fault-injection nemesis composition for RustFS Jepsen runs.

  Composes official `jepsen.nemesis.combined/nemesis-package`:
    - :partition -> network splits (halves, majorities-ring, random node)
    - :kill      -> SIGKILL + restart of rustfs server processes
    - :pause     -> SIGSTOP / SIGCONT process stalls (GC/IO pause simulation)
    - :packet    -> tc netem packet delay, loss, reorder, and corruption
    - :clock     -> clock skew bumps (VM / bare-metal nodes only)

  Plus a custom RustFS-specific `:drive` nemesis that takes a single drive
  directory unreadable (`chmod 000`) and later restores it (`chmod 0755`) to
  verify erasure-coded degraded operation and online recovery."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.control :as c]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.nemesis.combined :as nc]
            [rustfs.jepsen.topology :as topo]))

(def standard-faults
  #{:partition :kill :pause :packet :clock})

(defn drive-fault-nemesis
  "Faults one random drive directory on a random node via `chmod 000`, then
  restores permissions on `:restore-drive` and teardown."
  []
  (let [faulted (atom #{})]
    (reify nemesis/Nemesis
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
                          (c/su (c/exec :chmod "0000" dir))))
            (swap! faulted conj [node dir])
            (info "Faulted drive" node dir)
            (assoc op :value {:node node :dir dir}))

          :restore-drive
          (let [targets @faulted]
            (doseq [[node dir] targets]
              (c/on-nodes test [node]
                          (fn [_ _]
                            (c/su (c/exec :chmod "0755" dir)))))
            (reset! faulted #{})
            (info "Restored faulted drives" targets)
            (assoc op :value (vec targets)))))

      (teardown! [_ test]
        (doseq [[node dir] @faulted]
          (c/on-nodes test [node]
                      (fn [_ _]
                        (c/su (try (c/exec :chmod "0755" dir) (catch Throwable _))))))
        (reset! faulted #{})))))

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

(defn build-nemesis-package
  "Construct a combined Jepsen nemesis package for the selected `:faults` set."
  [opts]
  (let [faults (set (:faults opts))
        interval (:nemesis-interval opts 12)
        combined-faults (filter standard-faults faults)
        std-pkg (nc/nemesis-package
                 {:db (:db opts)
                  :nodes (:nodes opts)
                  :faults combined-faults
                  :partition {:targets [:one :primaries :majority :majorities-ring]}
                  :kill {:targets [:one]}
                  :pause {:targets [:one]}
                  :packet {:targets [:one :minority-third]
                           :behaviors [{:delay {:time :100ms :jitter :40ms}}
                                       {:loss {:percent :15%}}]}
                  :clock {:targets [:one]}
                  :interval interval})
        drv-pkg (drive-fault-package {:faults faults :interval interval})]
    (if drv-pkg
      (nc/compose-packages [std-pkg drv-pkg])
      std-pkg)))
