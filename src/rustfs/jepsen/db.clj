(ns rustfs.jepsen.db
  "Jepsen DB lifecycle for RustFS clusters.

  Supports:
    - Installing official RustFS GitHub release archives (pinned SHA-256 for
      1.0.7 linux x86_64/aarch64 musl & gnu zips)
    - Deploying a prebuilt local RustFS binary (`--local-binary`)
    - Starting/stopping `rustfs server` via `start-stop-daemon` (avoiding
      systemd `ProtectClock=true` and `Restart=always` which interfere with
      Jepsen clock & kill nemeses)
    - Implementing `db/DB`, `db/Kill`, `db/Pause`, and `db/LogFiles`"
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.db :as db]
            [jepsen.util :as util]
            [rustfs.jepsen.s3 :as s3]
            [rustfs.jepsen.topology :as topo]))

(def binary-path "/usr/local/bin/rustfs")
(def pid-file "/var/run/rustfs.pid")
(def log-file "/var/log/rustfs/rustfs.log")

(def release-assets
  "Pinned SHA-256 digests from the official RustFS 1.0.7 GitHub release."
  {"1.0.7"
   {"x86_64"
    {:url "https://github.com/rustfs/rustfs/releases/download/1.0.7/rustfs-linux-x86_64-musl-v1.0.7.zip"
     :sha256 "c90a40ba8cdc1468c71029a4d65fdd13449720183d3a8e00bc61ab09fcd448a5"}
    "aarch64"
    {:url "https://github.com/rustfs/rustfs/releases/download/1.0.7/rustfs-linux-aarch64-musl-v1.0.7.zip"
     :sha256 "e2c6d45105dd05df2f35547fbebfacf40dfba6ec4f8c31b86e30934faea79e76"}}})

(defn detect-arch!
  []
  (let [raw (str/trim (c/exec :uname :-m))]
    (case raw
      ("x86_64" "amd64") "x86_64"
      ("aarch64" "arm64") "aarch64"
      (throw (ex-info "Unsupported Linux CPU architecture for RustFS Jepsen node."
                      {:arch raw})))))

(defn install-release!
  [test]
  (if-let [local-bin (:local-binary test)]
    (do
      (info "Uploading local RustFS binary" local-bin)
      (c/upload local-bin binary-path)
      (c/exec :chmod :+x binary-path))
    (let [version (:version test)
          arch (detect-arch!)
          asset (or (get-in release-assets [version arch])
                    (throw (ex-info "No pinned RustFS release archive for version/arch."
                                    {:version version :arch arch})))
          zip (cu/cached-wget! (:url asset))
          actual-sha (-> (c/exec :sha256sum zip) (str/split #"\s+") first)]
      (when-not (= (:sha256 asset) actual-sha)
        (c/exec :rm :-f zip)
        (throw (ex-info "RustFS release archive SHA-256 mismatch."
                        {:url (:url asset)
                         :expected (:sha256 asset)
                         :actual actual-sha})))
      (let [tmp (cu/tmp-dir!)]
        (try
          (c/exec :unzip :-qo zip :-d tmp)
          (let [paths (cu/ls tmp {:recursive? true :full-path? true})
                bin (or (first (filter #(= "rustfs" (last (str/split % #"/"))) paths))
                        (throw (ex-info "Release archive does not contain rustfs binary."
                                        {:paths paths})))]
            (c/exec :install :-m "0755" bin binary-path))
          (finally
            (c/exec :rm :-rf tmp))))
      (info "Installed RustFS" version arch "to" binary-path))))

(defn prepare-drives!
  [test]
  (let [drives (:drives-per-node (get topo/topologies (:topology test)))]
    (c/exec :mkdir :-p "/var/log/rustfs" "/var/lib/rustfs")
    (c/exec :rm :-f log-file)
    (doseq [i (range 1 (inc drives))]
      (let [dir (topo/data-dir i)]
        (c/exec :mkdir :-p dir)
        (c/exec :chmod "0755" dir)
        (c/exec :find dir :-mindepth 1 :-delete)))))

(defn write-credentials!
  [test]
  (c/exec :mkdir :-p "/etc/rustfs")
  (cu/write-file! (:access-key test) "/etc/rustfs/access-key")
  (cu/write-file! (:secret-key test) "/etc/rustfs/secret-key")
  (c/exec :chmod "0600" "/etc/rustfs/access-key" "/etc/rustfs/secret-key"))

(def default-rpc-secret
  "Not `rustfsadmin`. RustFS refuses to derive an internode RPC token when
  either half of the credential pair is still the public default, and it also
  rejects an RPC secret equal to that default."
  "jepsen-rpc-secret")

(defn should-bypass-disk-check?
  "RustFS rejects multiple data directories on one physical device unless
  RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true. Docker binds every dataN directory to
  one volume per node, so those device ids match. Distinct device ids (separate
  drives on a VM) keep the check enabled."
  [device-ids]
  (boolean (and (> (count device-ids) 1)
                (every? #(and (string? %) (not (str/blank? %))) device-ids)
                (apply = device-ids))))

(defn data-device-ids!
  "GNU `stat` device id (`st_dev`) for each topology data directory."
  [test]
  (let [drives (:drives-per-node (get topo/topologies (:topology test)))]
    (mapv (fn [i]
            (str/trim (str (c/exec :stat :-c "%d" (topo/data-dir i)))))
          (range 1 (inc drives)))))

(defn start-node!
  [test node]
  (let [volumes (topo/volumes-arg (:nodes test) (:topology test) (:s3-port test))
        rpc-secret (:rpc-secret test default-rpc-secret)
        bypass? (should-bypass-disk-check? (data-device-ids! test))]
    (when (or (str/blank? rpc-secret)
              (= "rustfsadmin" rpc-secret))
      (throw (ex-info "RUSTFS_RPC_SECRET must be non-empty and must not be the default rustfsadmin credential."
                      {:node node})))
    (when bypass?
      (info node "data directories share one device; enabling RUSTFS_UNSAFE_BYPASS_DISK_CHECK for this local layout"))
    (info node "starting rustfs server with RUSTFS_VOLUMES=" volumes)
    (write-credentials! test)
    (cu/start-daemon!
     {:env (cond-> {:RUSTFS_ACCESS_KEY_FILE "/etc/rustfs/access-key"
                    :RUSTFS_SECRET_KEY_FILE "/etc/rustfs/secret-key"
                    :RUSTFS_RPC_SECRET rpc-secret
                    :RUSTFS_ADDRESS (str "0.0.0.0:" (:s3-port test))
                    :RUSTFS_VOLUMES volumes
                    :RUSTFS_CONSOLE_ENABLE "false"
                    :RUSTFS_REGION "us-east-1"
                    :RUST_LOG (:rust-log test "info")}
             bypass? (assoc :RUSTFS_UNSAFE_BYPASS_DISK_CHECK "true"))
      :logfile log-file
      :pidfile pid-file
      :chdir "/var/lib/rustfs"
      :make-pidfile? true}
     binary-path
     "server")))

(defn stop-node!
  [_test node]
  (info node "stopping rustfs server")
  (cu/stop-daemon! "rustfs" pid-file)
  (try (c/exec :pkill :-9 :-x "rustfs") (catch Throwable _)))

(defn wait-ready!
  [test node]
  (let [endpoint (topo/endpoint-url node (:s3-port test))
        client (s3/make-client {:endpoint endpoint
                                :access-key (:access-key test)
                                :secret-key (:secret-key test)
                                :timeout-ms 2000})
        deadline (+ (System/currentTimeMillis) (* 1000 (:ready-timeout-sec test 60)))]
    (try
      (loop []
        (let [ok? (try
                    (s3/list-objects! client (:bucket test))
                    true
                    (catch Throwable e
                      (let [cls (:class (s3/classify-exception e))]
                        ;; 404 NoSuchBucket means S3 API is up and answering!
                        (= cls :not-found))))]
          (cond
            ok?
            (info node "RustFS S3 API ready at" endpoint)

            (> (System/currentTimeMillis) deadline)
            (throw (ex-info "Timed out waiting for RustFS S3 readiness."
                            {:node node :endpoint endpoint}))

            :else
            (do
              (Thread/sleep 500)
              (recur)))))
      (finally
        (s3/close-client! client)))))

(defn db
  "Construct the Jepsen DB implementation for RustFS."
  []
  (reify
    db/DB
    (setup! [_ test node]
      (c/su
       (install-release! test)
       (prepare-drives! test)
       (start-node! test node))
      (wait-ready! test node))

    (teardown! [_ test node]
      (c/su
       (stop-node! test node)
       (let [drives (:drives-per-node (get topo/topologies (:topology test)))]
         (doseq [i (range 1 (inc drives))]
           (let [dir (topo/data-dir i)]
             (try (c/exec :chmod "0755" dir) (catch Throwable _))
             (try (c/exec :find dir :-mindepth 1 :-delete) (catch Throwable _)))))))

    db/LogFiles
    (log-files [_ _test _node]
      {log-file "rustfs.log"})

    db/Kill
    (start! [_ test node]
      (c/su (start-node! test node))
      (try
        (wait-ready! test node)
        (catch Throwable e
          (warn e node "restarted but readiness probe timed out during active fault")))
      :started)

    (kill! [_ test node]
      (c/su (stop-node! test node))
      :killed)

    db/Pause
    (pause! [_ _test node]
      (c/su (c/exec :pkill :-STOP :-x "rustfs"))
      :paused)

    (resume! [_ _test node]
      (c/su (c/exec :pkill :-CONT :-x "rustfs"))
      :resumed)))
