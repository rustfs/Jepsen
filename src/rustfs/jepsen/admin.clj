(ns rustfs.jepsen.admin
  "RustFS Admin v3 (`/rustfs/admin/v3/...`), STS/OIDC, and S3 Tables REST
  helper client for enterprise Jepsen workloads.

  Provides SigV4-authenticated HTTP calls for:
    - Site replication (`/rustfs/admin/v3/site-replication/...`)
    - Pool expansion, decommission, rebalance (`/rustfs/admin/v3/pools/...`, `/rebalance/...`)
    - Remote tiering (`/rustfs/admin/v3/tier`)
    - Bucket capacity quota (`/rustfs/admin/v3/quota`)
    - Audit & notification runtime configuration
    - On-demand migration (`/rustfs/admin/v3/on-demand-migration/...`)
    - OIDC / STS `AssumeRoleWithWebIdentity`
    - S3 Tables REST catalog (`/buckets`, `/namespaces`, `/tables`, metadata-location CAS)"
  (:require [clojure.string :as str])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
           [java.nio.charset StandardCharsets]
           [java.time Duration]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]))

(defn- hmac-sha256
  [^bytes key ^String data]
  (let [mac (Mac/getInstance "HmacSHA256")]
    (.init mac (SecretKeySpec. key "HmacSHA256"))
    (.doFinal mac (.getBytes data StandardCharsets/UTF_8))))

(defn- hex-bytes
  [^bytes bs]
  (apply str (map #(format "%02x" (bit-and % 0xff)) bs)))

(defn make-http-client
  [timeout-ms]
  (-> (HttpClient/newBuilder)
      (.connectTimeout (Duration/ofMillis (or timeout-ms 5000)))
      (.build)))

(defn admin-request!
  "Issue an HTTP request to a RustFS Admin / STS / S3 Tables REST endpoint.
  Returns {:status int :body string :headers map}."
  [{:keys [^HttpClient http endpoint method path body headers timeout-ms]
    :or {method "GET" body "" headers {} timeout-ms 5000}}]
  (let [url (str (str/replace endpoint #"/$" "") path)
        b (-> (HttpRequest/newBuilder (URI/create url))
              (.timeout (Duration/ofMillis timeout-ms))
              (.method (str/upper-case method)
                       (if (str/blank? body)
                         (HttpRequest$BodyPublishers/noBody)
                         (HttpRequest$BodyPublishers/ofString body StandardCharsets/UTF_8))))
        _ (doseq [[k v] headers]
            (.header b (str k) (str v)))
        resp (.send http (.build b) (HttpResponse$BodyHandlers/ofString StandardCharsets/UTF_8))]
    {:status (.statusCode resp)
     :body (.body resp)
     :headers (into {} (map (fn [[k v]] [(str/lower-case k) (vec v)])
                            (.map (.headers resp))))}))

(defn sigv4-derive-key
  "Derive AWS SigV4 signing key bytes for `(secret-key, date-stamp, region, service)`."
  [secret-key date-stamp region service]
  (-> (.getBytes (str "AWS4" secret-key) StandardCharsets/UTF_8)
      (hmac-sha256 date-stamp)
      (hmac-sha256 region)
      (hmac-sha256 service)
      (hmac-sha256 "aws4_request")
      hex-bytes))

;; Canonical RustFS Admin v3 API endpoint templates
(def paths
  {:info                  "/rustfs/admin/v3/info"
   :site-repl-status      "/rustfs/admin/v3/site-replication/status"
   :site-repl-info        "/rustfs/admin/v3/site-replication/info"
   :pool-decommission     "/rustfs/admin/v3/pools/decommission"
   :pool-status           "/rustfs/admin/v3/pools/status"
   :rebalance-start       "/rustfs/admin/v3/rebalance/start"
   :rebalance-status      "/rustfs/admin/v3/rebalance/status"
   :tier                  "/rustfs/admin/v3/tier"
   :quota                 "/rustfs/admin/v3/quota"
   :audit-targets         "/rustfs/admin/v3/audit/targets"
   :oidc-config           "/rustfs/admin/v3/oidc"
   :on-demand-migration   "/rustfs/admin/v3/on-demand-migration"
   :s3-tables-buckets     "/buckets"})
