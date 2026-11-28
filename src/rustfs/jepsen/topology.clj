(ns rustfs.jepsen.topology
  "Official RustFS distributed erasure-coding topologies, quorum formulas, and
  RUSTFS_VOLUMES ellipsis expansion.

  Matches rustfs/rustfs crates/ecstore:
    - default_parity_count: 1->0, 2..3->1, 4..5->2, 6..7->3, >=8->4
    - read_quorum:  drives - parity
    - write_quorum: data + 1 if data == parity else data

  Supported topologies:
    - :4x4 -> 4 nodes x 4 drives = 16 drives (EC:4, data=12, R=12, W=12)
    - :3x2 -> 3 nodes x 2 drives =  6 drives (EC:3, data=3,  R=3,  W=4)
    - :4x2 -> 4 nodes x 2 drives =  8 drives (EC:4, data=4,  R=4,  W=5)"
  (:require [clojure.string :as str]))

(def default-jepsen-nodes
  "Default 5-node list injected by Jepsen CLI when -n is omitted."
  ["n1" "n2" "n3" "n4" "n5"])

(defn default-parity-count
  "Exact RustFS default parity shard count for an erasure set of `drive-count` drives."
  [drive-count]
  (cond
    (<= drive-count 1) 0
    (<= drive-count 3) 1
    (<= drive-count 5) 2
    (<= drive-count 7) 3
    :else 4))

(defn topology-spec
  "Compute a complete topology map from node count and drives per node."
  [kw nodes drives-per-node summary]
  (let [total (* nodes drives-per-node)
        parity (default-parity-count total)
        data (- total parity)
        read-q data
        write-q (if (= data parity) (inc data) data)]
    {:id kw
     :nodes nodes
     :drives-per-node drives-per-node
     :drives total
     :set-size total
     :parity parity
     :data-shards data
     :read-quorum read-q
     :write-quorum write-q
     :lock-quorum (inc (quot nodes 2))
     :survives-one-node-loss? (>= (- total drives-per-node) write-q)
     :summary summary}))

(def topologies
  {:4x4 (topology-spec :4x4 4 4
                       "4x4 (16 drives, EC:4, R=12, W=12): tolerates 1 full node loss; 2-2 split halts writes safely.")
   :3x2 (topology-spec :3x2 3 2
                       "3x2 (6 drives, EC:3, R=3, W=4): 2-node majority (4 drives) retains R/W quorum; isolated node cannot read or write.")
   :4x2 (topology-spec :4x2 4 2
                       "4x2 (8 drives, EC:4, R=4, W=5): tolerates 1 full node loss (6 drives >= W=5); 2-2 split (4 drives) rejects writes.")})

(defn node-name
  [node]
  (cond
    (keyword? node) (name node)
    (symbol? node) (name node)
    :else (str node)))

(defn data-dir
  "On-node drive mount directory for 1-based drive index `i`."
  [i]
  (str "/var/lib/rustfs/data" i))

(defn parse-numbered-host
  "Split a numbered hostname such as `n1.jepsen.local` into {:prefix :number :suffix}."
  [host]
  (let [s (node-name host)]
    (if-let [[_ prefix digits suffix] (re-matches #"^(.*?)(\d+)([^\d]*)$" s)]
      {:host s
       :prefix prefix
       :number (Long/parseLong digits)
       :suffix suffix}
      (throw (ex-info "Host must contain a numeric index so RUSTFS_VOLUMES can use {start...end}."
                      {:host s})))))

(defn default-nodes
  ([topology host-template]
   (default-nodes topology host-template 1))
  ([topology host-template pools]
   (let [spec (get topologies topology)
         total-nodes (* (:nodes spec 0) (max 1 (long (or pools 1))))]
     (when-not spec
       (throw (ex-info "Unknown RustFS topology." {:topology topology :known (keys topologies)})))
     (mapv #(format host-template %) (range 1 (inc total-nodes))))))

(defn resolve-nodes
  "Use explicit `-n` nodes when they match `topology * pools`; replace Jepsen's
  untouched default 5-node list with the canonical hostnames."
  ([requested topology host-template]
   (resolve-nodes requested topology host-template 1))
  ([requested topology host-template pools]
   (let [spec (get topologies topology)
         pools (max 1 (long (or pools 1)))
         expected (* (:nodes spec 0) pools)
         names (mapv node-name requested)]
     (when-not spec
       (throw (ex-info "Unknown RustFS topology." {:topology topology :known (keys topologies)})))
     (cond
       (= names default-jepsen-nodes)
       (default-nodes topology host-template pools)

       (= (count names) expected)
       names

       :else
       (throw (ex-info "Requested node count does not match topology * pools."
                       {:topology topology
                        :pools pools
                        :expected expected
                        :actual (count names)
                        :nodes names}))))))

(defn- single-pool-ellipsis
  [pool-nodes spec port]
  (let [parsed (sort-by :number (map parse-numbered-host pool-nodes))
        first-h (first parsed)
        last-h (last parsed)
        prefixes (set (map :prefix parsed))
        suffixes (set (map :suffix parsed))
        nums (map :number parsed)
        contiguous? (= nums (range (:number first-h) (inc (:number last-h))))]
    (when-not (= (count pool-nodes) (:nodes spec))
      (throw (ex-info "Node count mismatch when building RUSTFS_VOLUMES."
                      {:topology (:id spec) :expected (:nodes spec) :actual (count pool-nodes)})))
    (when-not (and (= 1 (count prefixes)) (= 1 (count suffixes)) contiguous?)
      (throw (ex-info "Nodes must form a contiguous numbered range for RustFS ellipsis expansion."
                      {:nodes pool-nodes})))
    (format "http://%s{%d...%d}%s:%d/var/lib/rustfs/data{1...%d}"
            (:prefix first-h)
            (:number first-h)
            (:number last-h)
            (:suffix first-h)
            port
            (:drives-per-node spec))))

(defn volumes-arg
  "Build single-pool or multi-pool space-separated RustFS ellipsis volume arguments, e.g.:
  - 1 pool (4x4): `http://n{1...4}:9000/var/lib/rustfs/data{1...4}`
  - 2 pools (3x2): `http://n{1...3}:9000/var/lib/rustfs/data{1...2} http://n{4...6}:9000/var/lib/rustfs/data{1...2}`"
  ([nodes topology port]
   (volumes-arg nodes topology port 1))
  ([nodes topology port pools]
   (let [spec (get topologies topology)
         pools (max 1 (long (or pools 1)))
         per-pool (:nodes spec 0)
         expected (* per-pool pools)]
     (when-not spec
       (throw (ex-info "Unknown RustFS topology." {:topology topology})))
     (when-not (= (count nodes) expected)
       (throw (ex-info "Node count mismatch when building RUSTFS_VOLUMES."
                       {:topology topology :pools pools :expected expected :actual (count nodes)})))
     (->> (partition per-pool nodes)
          (map #(single-pool-ellipsis % spec port))
          (str/join " ")))))

(defn endpoint-url
  [node port]
  (str "http://" (node-name node) ":" port))
