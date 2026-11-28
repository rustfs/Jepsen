(ns rustfs.jepsen.s3-wire-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [rustfs.jepsen.admin :as admin]
            [rustfs.jepsen.s3 :as s3])
  (:import [com.sun.net.httpserver HttpExchange HttpHandler HttpServer]
           [java.net InetSocketAddress]
           [java.nio.charset StandardCharsets]
           [java.time Instant]
           [software.amazon.awssdk.core.exception SdkClientException]
           [software.amazon.awssdk.services.s3.model S3Exception]))

(defn- xml-response!
  [^HttpExchange exchange status xml]
  (let [body (.getBytes ^String xml StandardCharsets/UTF_8)]
    (.set (.getResponseHeaders exchange) "Content-Type" "application/xml")
    (.sendResponseHeaders exchange status (alength body))
    (with-open [output (.getResponseBody exchange)]
      (.write output body))))

(defn- with-s3-server
  [respond! run-test!]
  (let [server (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)
        requests (atom [])]
    (.createContext server "/"
                    (reify HttpHandler
                      (handle [_ exchange]
                        (with-open [^HttpExchange exchange exchange]
                          (let [request {:method (.getRequestMethod exchange)
                                         :query (.getQuery (.getRequestURI exchange))
                                         :headers (into {} (map (fn [[k v]] [(str/lower-case k) (vec v)])
                                                                (.getRequestHeaders exchange)))}]
                            (with-open [body (.getRequestBody exchange)]
                              (.readAllBytes body))
                            (let [attempt (count (swap! requests conj request))]
                              (respond! exchange request attempt)))))))
    (.start server)
    (try
      (with-open [client (s3/make-client {:endpoint (str "http://127.0.0.1:" (.getPort (.getAddress server)))
                                         :access-key "test-access-key"
                                         :secret-key "test-secret-key"
                                         :timeout-ms 2000})]
        (run-test! client requests))
      (finally
        (.stop server 0)))))

(deftest mutations-have-one-wire-attempt
  (testing "a retryable service response cannot be replaced by a precondition failure"
    (with-s3-server
      (fn [exchange _ attempt]
        (if (= 1 attempt)
          (xml-response! exchange 503 "<Error><Code>SlowDown</Code></Error>")
          (xml-response! exchange 412 "<Error><Code>PreconditionFailed</Code></Error>")))
      (fn [client requests]
        (let [result (try
                       (s3/put-object! client "bucket" "key" "payload" :if-none-match "*")
                       (catch S3Exception e (s3/classify-exception e)))]
          (is (= {:status 503 :class :indeterminate}
                 (select-keys result [:status :class])))
          (is (= ["PUT"] (mapv :method @requests)))))))
  (testing "a bodyless delete with a dropped reply cannot be replayed by the HTTP transport"
    (with-s3-server
      (fn [exchange _ attempt]
        (when (> attempt 1)
          (.sendResponseHeaders ^HttpExchange exchange 204 -1)))
      (fn [client requests]
        (is (thrown? SdkClientException (s3/delete-object! client "bucket" "key")))
        (is (= ["DELETE"] (mapv :method @requests)))))))

(defn- list-page
  [key truncated? token]
  (str "<ListBucketResult><IsTruncated>" truncated? "</IsTruncated>"
       (when token (str "<NextContinuationToken>" token "</NextContinuationToken>"))
       "<Contents><Key>" key "</Key><Size>1</Size></Contents></ListBucketResult>"))

(deftest list-pagination-handles-opaque-tokens
  (with-s3-server
    (fn [exchange {:keys [query]} attempt]
      (cond
        (= 1 attempt) (xml-response! exchange 200 (list-page "first" true "opaque+/="))
        (and (= 2 attempt) (str/includes? query "continuation-token=opaque+/="))
        (xml-response! exchange 200 (list-page "second" false nil))
        :else (xml-response! exchange 400 "<Error><Code>InvalidArgument</Code></Error>")))
    (fn [client requests]
      (is (= ["first" "second"] (mapv :key (s3/list-objects! client "bucket" :prefix "prefix"))))
      (is (= 2 (count @requests)))
      (is (every? #(str/includes? (:query %) "prefix=prefix") @requests)))))

(deftest list-pagination-rejects-missing-and-cyclic-tokens
  (doseq [tokens [[nil] ["a" "b" "a"]]]
    (with-s3-server
      (fn [exchange _ attempt]
        (let [truncated? (<= attempt (count tokens))]
          (xml-response! exchange 200
                         (list-page "key" truncated? (get tokens (dec attempt))))))
      (fn [client requests]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"missing or repeated continuation token"
                              (s3/list-objects! client "bucket")))
        (is (= (count tokens) (count @requests)))))))

(defn- version-page
  [version-id truncated? [next-key next-version]]
  (str "<ListVersionsResult><IsTruncated>" truncated? "</IsTruncated>"
       (when next-key (str "<NextKeyMarker>" next-key "</NextKeyMarker>"))
       (when next-version (str "<NextVersionIdMarker>" next-version "</NextVersionIdMarker>"))
       "<Version><Key>key</Key><VersionId>" version-id "</VersionId></Version>"
       "</ListVersionsResult>"))

(deftest version-pagination-uses-both-markers
  (with-s3-server
    (fn [exchange {:keys [query]} attempt]
      (cond
        (= 1 attempt) (xml-response! exchange 200 (version-page "new" true ["key" "new"]))
        (and (= 2 attempt) (str/includes? query "key-marker=key")
             (str/includes? query "version-id-marker=new"))
        (xml-response! exchange 200 (version-page "null" true ["key" "null"]))
        (and (= 3 attempt) (str/includes? query "key-marker=key")
             (str/includes? query "version-id-marker=null"))
        (xml-response! exchange 200 (version-page "old" false nil))
        :else (xml-response! exchange 400 "<Error><Code>InvalidArgument</Code></Error>")))
    (fn [client requests]
      (is (= ["new" "null" "old"] (mapv :version-id (:versions (s3/list-versions! client "bucket")))))
      (is (= 3 (count @requests))))))

(deftest version-pagination-rejects-missing-and-cyclic-markers
  (doseq [markers [[nil] [["key" "a"] ["key" "b"] ["key" "a"]]]]
    (with-s3-server
      (fn [exchange _ attempt]
        (let [truncated? (<= attempt (count markers))]
          (xml-response! exchange 200
                         (version-page "version" truncated? (get markers (dec attempt))))))
      (fn [client requests]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"missing or repeated marker"
                              (s3/list-versions! client "bucket")))
        (is (= (count markers) (count @requests)))))))

(deftest ensure-bucket-object-lock-and-admin-sigv4-wire-test
  (testing "ensure-bucket! with :object-lock? true sends x-amz-bucket-object-lock-enabled header"
    (with-s3-server
      (fn [exchange {:keys [method]} _]
        (if (= "HEAD" method)
          (xml-response! exchange 404 "<Error><Code>NoSuchBucket</Code></Error>")
          (xml-response! exchange 200 "")))
      (fn [client requests]
        (s3/ensure-bucket! client "worm-bkt" :object-lock? true)
        (let [create-req (second @requests)]
          (is (= "PUT" (:method create-req)))
          (is (= ["true"] (get-in create-req [:headers "x-amz-bucket-object-lock-enabled"])))))))

  (testing "admin-request! attaches deterministic AWS4-HMAC-SHA256 Authorization, x-amz-date, and content hash"
    (let [fixed-instant (Instant/parse "2026-11-28T04:00:00Z")
          sig1 (admin/sign-sigv4-headers {:method "GET"
                                          :uri "http://127.0.0.1:9000/rustfs/admin/v3/scanner/status?bucket=test"
                                          :access-key "rustfsadmin"
                                          :secret-key "rustfsadmin"
                                          :instant fixed-instant})
          sig2 (admin/sign-sigv4-headers {:method "GET"
                                          :uri "http://127.0.0.1:9000/rustfs/admin/v3/scanner/status?bucket=test"
                                          :access-key "rustfsadmin"
                                          :secret-key "rustfsadmin"
                                          :instant fixed-instant})]
      (is (= sig1 sig2))
      (is (= "20261128T040000Z" (get sig1 "x-amz-date")))
      (is (str/starts-with? (get sig1 "Authorization")
                            "AWS4-HMAC-SHA256 Credential=rustfsadmin/20261128/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=")))
    (let [srv (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)
          seen-auth (atom nil)]
      (.createContext srv "/"
                      (reify HttpHandler
                        (handle [_ exch]
                          (reset! seen-auth (.getFirst (.getRequestHeaders exch) "Authorization"))
                          (xml-response! exch 200 "{}"))))
      (.start srv)
      (try
        (let [http (admin/make-http-client 2000)
              resp (admin/admin-request! {:http http
                                          :endpoint (str "http://127.0.0.1:" (.getPort (.getAddress srv)))
                                          :path "/rustfs/admin/v3/scanner/status"
                                          :access-key "rustfsadmin"
                                          :secret-key "rustfsadmin"})]
          (is (= 200 (:status resp)))
          (is (str/starts-with? (str @seen-auth) "AWS4-HMAC-SHA256 ")))
        (finally
          (.stop srv 0))))))
