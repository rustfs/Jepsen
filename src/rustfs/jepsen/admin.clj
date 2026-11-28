(ns rustfs.jepsen.admin
  "RustFS Admin v3 (`/rustfs/admin/v3/...`), STS/OIDC, and S3 Tables REST
  helper client with full AWS Signature Version 4 (`AWS4-HMAC-SHA256`) request
  signing."
  (:require [clojure.string :as str])
  (:import [java.net URI]
           [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.time Duration Instant ZoneOffset ZonedDateTime]
           [java.time.format DateTimeFormatter]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec]))

(def ^:private amz-date-fmt
  (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'"))

(def ^:private date-stamp-fmt
  (DateTimeFormatter/ofPattern "yyyyMMdd"))

(defn- hmac-sha256
  ^bytes [^bytes key ^String data]
  (let [mac (Mac/getInstance "HmacSHA256")]
    (.init mac (SecretKeySpec. key "HmacSHA256"))
    (.doFinal mac (.getBytes data StandardCharsets/UTF_8))))

(defn- hex-bytes
  ^String [^bytes bs]
  (apply str (map #(format "%02x" (bit-and % 0xff)) bs)))

(defn- sha256-hex
  ^String [^bytes bs]
  (let [md (MessageDigest/getInstance "SHA-256")]
    (.update md bs)
    (hex-bytes (.digest md))))

(defn sigv4-derive-key-bytes
  "Derive raw 32-byte AWS SigV4 signing key for `(secret-key, date-stamp, region, service)`."
  ^bytes [secret-key date-stamp region service]
  (-> (.getBytes (str "AWS4" secret-key) StandardCharsets/UTF_8)
      (hmac-sha256 date-stamp)
      (hmac-sha256 region)
      (hmac-sha256 service)
      (hmac-sha256 "aws4_request")))

(defn sigv4-derive-key
  "Derive hex-encoded AWS SigV4 signing key for `(secret-key, date-stamp, region, service)`."
  [secret-key date-stamp region service]
  (hex-bytes (sigv4-derive-key-bytes secret-key date-stamp region service)))

(defn- canonical-query-string
  [^String raw-query]
  (if (str/blank? raw-query)
    ""
    (->> (str/split raw-query #"&")
         (remove str/blank?)
         (map (fn [pair]
                (let [[k v] (str/split pair #"=" 2)]
                  [k (or v "")])))
         (sort-by (juxt first second))
         (map (fn [[k v]] (str k "=" v)))
         (str/join "&"))))

(defn- host-header
  [^URI uri]
  (let [h (.getHost uri)
        p (.getPort uri)
        scheme (str/lower-case (or (.getScheme uri) "http"))]
    (if (or (neg? p)
            (and (= scheme "http") (= p 80))
            (and (= scheme "https") (= p 443)))
      h
      (str h ":" p))))

(defn sign-sigv4-headers
  "Compute AWS SigV4 authentication headers (`x-amz-date`, `x-amz-content-sha256`,
  and `Authorization`) for an HTTP request."
  [{:keys [method uri body access-key secret-key region service instant]
    :or {method "GET" body "" region "us-east-1" service "s3"}}]
  (let [^URI u (if (instance? URI uri) uri (URI/create (str uri)))
        ^ZonedDateTime zdt (.atZone ^Instant (or instant (Instant/now)) ZoneOffset/UTC)
        amz-date (.format amz-date-fmt zdt)
        date-stamp (.format date-stamp-fmt zdt)
        body-bytes (if (bytes? body)
                     ^bytes body
                     (.getBytes (str (or body "")) StandardCharsets/UTF_8))
        payload-hash (sha256-hex body-bytes)
        host (host-header u)
        canonical-uri (let [p (.getRawPath u)] (if (str/blank? p) "/" p))
        canonical-query (canonical-query-string (.getRawQuery u))
        canonical-headers (str "host:" host "\n"
                               "x-amz-content-sha256:" payload-hash "\n"
                               "x-amz-date:" amz-date "\n")
        signed-headers "host;x-amz-content-sha256;x-amz-date"
        canonical-req (str (str/upper-case method) "\n"
                           canonical-uri "\n"
                           canonical-query "\n"
                           canonical-headers "\n"
                           signed-headers "\n"
                           payload-hash)
        scope (str date-stamp "/" region "/" service "/aws4_request")
        string-to-sign (str "AWS4-HMAC-SHA256\n"
                            amz-date "\n"
                            scope "\n"
                            (sha256-hex (.getBytes canonical-req StandardCharsets/UTF_8)))
        signing-key (sigv4-derive-key-bytes secret-key date-stamp region service)
        signature (hex-bytes (hmac-sha256 signing-key string-to-sign))
        auth-header (str "AWS4-HMAC-SHA256 Credential=" access-key "/" scope
                         ", SignedHeaders=" signed-headers
                         ", Signature=" signature)]
    {"x-amz-date" amz-date
     "x-amz-content-sha256" payload-hash
     "Authorization" auth-header}))

(defn make-http-client
  [timeout-ms]
  (-> (HttpClient/newBuilder)
      (.connectTimeout (Duration/ofMillis (or timeout-ms 5000)))
      (.build)))

(defn admin-request!
  "Issue a SigV4-authenticated HTTP request to a RustFS Admin v3 / STS / S3
  Tables REST endpoint. Returns {:status int :body string :headers map}."
  [{:keys [^HttpClient http endpoint method path body headers access-key secret-key region service timeout-ms]
    :or {method "GET" body "" headers {} region "us-east-1" service "s3" timeout-ms 5000}}]
  (let [url (str (str/replace endpoint #"/$" "") path)
        uri (URI/create url)
        sig-headers (if (and (not (str/blank? access-key))
                             (not (str/blank? secret-key)))
                      (sign-sigv4-headers {:method method
                                           :uri uri
                                           :body body
                                           :access-key access-key
                                           :secret-key secret-key
                                           :region region
                                           :service service})
                      {})
        merged-headers (merge sig-headers headers)
        b (-> (HttpRequest/newBuilder uri)
              (.timeout (Duration/ofMillis timeout-ms))
              (.method (str/upper-case method)
                       (if (str/blank? body)
                         (HttpRequest$BodyPublishers/noBody)
                         (HttpRequest$BodyPublishers/ofString body StandardCharsets/UTF_8))))
        _ (doseq [[k v] merged-headers]
            (.header b (str k) (str v)))
        resp (.send http (.build b) (HttpResponse$BodyHandlers/ofString StandardCharsets/UTF_8))]
    {:status (.statusCode resp)
     :body (.body resp)
     :headers (into {} (map (fn [[k v]] [(str/lower-case k) (vec v)])
                            (.map (.headers resp))))}))

;; Canonical RustFS Admin v3 API endpoint routes
(def paths
  {:info                    "/rustfs/admin/v3/info"
   :pools-list              "/rustfs/admin/v3/pools/list"
   :pools-status            "/rustfs/admin/v3/pools/status"
   :pool-decommission-start "/rustfs/admin/v3/pools/decommission"
   :pool-cancel             "/rustfs/admin/v3/pools/cancel"
   :pool-clear              "/rustfs/admin/v3/pools/clear"
   :site-repl-status        "/rustfs/admin/v3/site-replication-info"
   :site-repl-info          "/rustfs/admin/v3/site-replication-info"
   :pool-decommission       "/rustfs/admin/v3/decommission/status"
   :pool-status             "/rustfs/admin/v3/decommission/status"
   :rebalance-start         "/rustfs/admin/v3/rebalance/start"
   :rebalance-stop          "/rustfs/admin/v3/rebalance/stop"
   :rebalance-status        "/rustfs/admin/v3/rebalance/status"
   :tier                    "/rustfs/admin/v3/tier"
   :quota                   "/rustfs/admin/v3/get-bucket-quota"
   :audit-targets           "/rustfs/admin/v3/arn"
   :oidc-config             "/rustfs/admin/v3/info"
   :on-demand-migration     "/rustfs/admin/v3/on-demand-migration"
   :s3-tables-buckets       "/buckets"
   :scanner-status          "/rustfs/admin/v3/scanner/status"
   :data-usage              "/rustfs/admin/v3/datausageinfo"
   :heal                    "/rustfs/admin/v3/heal"
   :mrf-status              "/rustfs/admin/v3/heal"
   :delete-recovery         "/rustfs/admin/v3/scanner/status"
   :bucket-quota-usage      "/rustfs/admin/v3/get-bucket-quota"})

(defn- capability-proof-transient?
  [{:keys [status body]}]
  (or (= status 503)
      (and (= status 500)
           (str/includes? (str body) "requires a live fleet capability proof"))))

(defn start-pool-decommission!
  "Trigger `POST /rustfs/admin/v3/pools/decommission?pool=<pool-id>&by-id=true`,
  retrying transient 503 / fleet-capability-proof responses up to `budget-ms`."
  [{:keys [http endpoint pool-id access-key secret-key budget-ms]
    :or {pool-id 0 budget-ms 60000}}]
  (let [path (format "%s?pool=%d&by-id=true" (:pool-decommission-start paths) (long pool-id))
        deadline (+ (System/currentTimeMillis) (long budget-ms))]
    (loop [delay-ms 300]
      (let [resp (admin-request! {:http http
                                  :endpoint endpoint
                                  :method "POST"
                                  :path path
                                  :access-key access-key
                                  :secret-key secret-key
                                  :timeout-ms 10000})]
        (cond
          (<= 200 (:status resp) 299)
          resp

          (and (capability-proof-transient? resp)
               (< (System/currentTimeMillis) deadline))
          (do
            (Thread/sleep delay-ms)
            (recur (min 2000 (* 2 delay-ms))))

          :else
          (throw (ex-info "Failed to start RustFS pool decommission."
                          {:path path :status (:status resp) :body (:body resp)})))))))

(defn start-pool-rebalance!
  "Trigger `POST /rustfs/admin/v3/rebalance/start`, retrying transient 503 /
  fleet-capability-proof responses up to `budget-ms`."
  [{:keys [http endpoint access-key secret-key budget-ms]
    :or {budget-ms 60000}}]
  (let [path (:rebalance-start paths)
        deadline (+ (System/currentTimeMillis) (long budget-ms))]
    (loop [delay-ms 300]
      (let [resp (admin-request! {:http http
                                  :endpoint endpoint
                                  :method "POST"
                                  :path path
                                  :access-key access-key
                                  :secret-key secret-key
                                  :timeout-ms 10000})]
        (cond
          (<= 200 (:status resp) 299)
          resp

          (and (capability-proof-transient? resp)
               (< (System/currentTimeMillis) deadline))
          (do
            (Thread/sleep delay-ms)
            (recur (min 2000 (* 2 delay-ms))))

          :else
          (throw (ex-info "Failed to start RustFS pool rebalance."
                          {:path path :status (:status resp) :body (:body resp)})))))))
