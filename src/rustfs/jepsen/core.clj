(ns rustfs.jepsen.core
  "CLI entry point for the official RustFS Jepsen verification suite."
  (:gen-class)
  (:require [clojure.string :as str]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.cli :as cli]
            [jepsen.generator :as gen]
            [jepsen.os.debian :as debian]
            [jepsen.tests :as tests]
            [rustfs.jepsen.db :as rdb]
            [rustfs.jepsen.nemesis :as rnem]
            [rustfs.jepsen.topology :as topo]
            [rustfs.jepsen.workloads :as rwork]))

(defn parse-csv-keywords
  [s]
  (if (or (nil? s) (str/blank? s) (= "none" (str/lower-case (str/trim s))))
    #{}
    (->> (str/split s #",")
         (map str/trim)
         (remove str/blank?)
         (map keyword)
         set)))

(def cli-opts
  [[nil "--topology TOPO" "RustFS cluster topology (4x4, 3x2, 4x2)."
    :default :4x4
    :parse-fn keyword
    :validate [topo/topologies (str "Must be one of: " (str/join ", " (map name (keys topo/topologies))))]]

   ["-w" "--workload NAME" "Workload to execute (5 core + 19 enterprise workloads)."
    :default :register
    :parse-fn keyword
    :validate [rwork/workloads (str "Must be one of: " (str/join ", " (sort (map name (keys rwork/workloads)))))]]

   [nil "--suite SUITE" "Workload suite for test-all (core, enterprise, all)."
    :default :core
    :parse-fn keyword
    :validate [#{:core :enterprise :all} "Must be one of: core, enterprise, all."]]

   [nil "--faults LIST" "Comma-separated nemesis faults (partition,kill,pause,packet,drive,clock,none)."
    :default #{:partition :kill :pause :packet}
    :parse-fn parse-csv-keywords]

   [nil "--version VER" "RustFS release version to install (when --local-binary is not set)."
    :default "1.0.7"]

   [nil "--local-binary PATH" "Path to a locally built Linux rustfs binary to deploy instead of downloading a release."
    :default nil]

   [nil "--host-template FMT" "Hostname template for auto-populated topology nodes."
    :default "n%d"]

   [nil "--s3-port PORT" "RustFS S3 HTTP port."
    :default 9000
    :parse-fn #(Long/parseLong %)]

   [nil "--access-key KEY" "RustFS root access key."
    :default "rustfsadmin"]

   [nil "--secret-key SECRET" "RustFS root secret key."
    :default "rustfsadmin"]

   [nil "--bucket NAME" "Target S3 bucket name for this test."
    :default "jepsen"]

   ["-r" "--rate HZ" "Target client request rate in operations per second."
    :default 10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number."]]

   [nil "--nemesis-interval SEC" "Seconds between nemesis fault injections."
    :default 12
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive."]]

   [nil "--key-count N" "Number of distinct object keys for multi-key workloads."
    :default 4
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive."]]

   [nil "--request-timeout-ms MS" "Per-request S3 HTTP/socket timeout in milliseconds."
    :default 5000
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive."]]])

(defn rustfs-test
  "Construct a complete Jepsen test map for RustFS."
  [opts]
  (let [nodes (topo/resolve-nodes (:nodes opts) (:topology opts) (:host-template opts))
        db-impl (rdb/db)
        opts' (assoc opts :nodes nodes :db db-impl)
        workload ((get rwork/workloads (:workload opts')) opts')
        nem-pkg (rnem/build-nemesis-package opts')
        client-gen (->> (:generator workload)
                        (gen/stagger (/ 1.0 (:rate opts'))))
        during-gen (->> (if-let [nem-gen (:generator nem-pkg)]
                          (gen/nemesis nem-gen client-gen)
                          client-gen)
                        (gen/time-limit (:time-limit opts')))
        recovery-gen (gen/phases
                      (gen/log "Healing all nemesis faults")
                      (when-let [final-nem (:final-generator nem-pkg)]
                        (gen/nemesis final-nem))
                      (gen/sleep 5))
        full-gen (if-let [fg (:final-generator workload)]
                   (gen/phases during-gen
                               recovery-gen
                               (gen/log "Running final post-heal verification")
                               (gen/clients fg))
                   (gen/phases during-gen recovery-gen))]
    (merge tests/noop-test
           opts'
           {:name (str "rustfs-" (name (:topology opts')) "-" (name (:workload opts')))
            :os debian/os
            :db db-impl
            :client (:client workload)
            :nemesis (:nemesis nem-pkg)
            :pure-generators true
            :plot {:nemeses (:perf nem-pkg)}
            :checker (checker/compose
                      {:perf (checker/perf {:nemeses (:perf nem-pkg)})
                       :stats (checker/stats)
                       :exceptions (checker/unhandled-exceptions)
                       :timeline (timeline/html)
                       :workload (:checker workload)})
            :generator full-gen})))

(defn all-tests
  "Matrix of workloads for `lein run test-all` selected by `--suite`."
  [opts]
  (let [selected (case (:suite opts :core)
                   :core (keys rwork/core-workloads)
                   :enterprise (sort (keys (dissoc rwork/workloads :register :append :versioned :multipart :list)))
                   :all (sort (keys rwork/workloads)))]
    (for [w selected]
      (rustfs-test (assoc opts :workload w)))))

(defn -main
  [& args]
  (cli/run!
   (merge (cli/single-test-cmd {:test-fn rustfs-test :opt-spec cli-opts})
          (cli/test-all-cmd {:tests-fn all-tests :opt-spec cli-opts})
          (cli/serve-cmd))
   args))
