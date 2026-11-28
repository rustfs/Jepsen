(ns rustfs.jepsen.s3
  "AWS SDK v2 path-style S3 client for RustFS Jepsen workloads.

  Carefully classifies S3 exceptions into:
    - :not-found     -> HTTP 404 (NoSuchKey / NoSuchBucket)
    - :precondition  -> HTTP 412 / 409 (If-Match / If-None-Match mismatch)
    - :definite-fail -> HTTP 400 / 403 / explicit pre-commit rejection
    - :indeterminate -> network timeouts, 500/503, connection resets (mapped to
                        Jepsen :info on writes so Knossos & Elle model ambiguous
                        commits accurately)."
  (:require [clojure.string :as str])
  (:import [java.net URI]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.time Duration]
           [software.amazon.awssdk.auth.credentials AwsBasicCredentials StaticCredentialsProvider]
           [software.amazon.awssdk.core.exception SdkClientException]
           [software.amazon.awssdk.core.sync RequestBody]
           [software.amazon.awssdk.http.urlconnection UrlConnectionHttpClient]
           [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.services.s3 S3Client S3Configuration]
           [software.amazon.awssdk.services.s3.model
            AbortMultipartUploadRequest
            BucketVersioningStatus
            CompleteMultipartUploadRequest
            CompletedMultipartUpload
            CompletedPart
            CreateBucketRequest
            CreateMultipartUploadRequest
            DeleteObjectRequest
            GetObjectRequest
            HeadBucketRequest
            ListObjectVersionsRequest
            ListObjectsV2Request
            NoSuchKeyException
            PutBucketVersioningRequest
            PutObjectRequest
            S3Exception
            UploadPartRequest
            VersioningConfiguration]))

(defn- digest-hex
  [^MessageDigest md]
  (apply str (map #(format "%02x" (bit-and % 0xff)) (.digest md))))

(defn sha256-hex
  "Compute lowercase hex SHA-256 of bytes or UTF-8 string."
  [data]
  (let [bytes (if (bytes? data)
                data
                (.getBytes (str data) StandardCharsets/UTF_8))
        md (MessageDigest/getInstance "SHA-256")]
    (.update md ^bytes bytes)
    (digest-hex md)))

(defn sha256-hex-chunks
  "SHA-256 of `chunks` concatenated, without allocating the concatenation."
  [chunks]
  (let [md (MessageDigest/getInstance "SHA-256")]
    (doseq [^bytes chunk chunks]
      (.update md chunk))
    (digest-hex md)))

(def s3-min-part-size
  "S3 minimum size of every non-final UploadPart. RustFS enforces the same 5 MiB floor."
  (* 5 1024 1024))

(defn multipart-part-bytes
  "Exactly `size` bytes for multipart part `part-number`, seeded by `seed`.
  The prefix is written inside the buffer so a 5 MiB part stays 5 MiB."
  ^bytes [seed part-number size]
  (let [size (int size)
        prefix (.getBytes (format "P%d-%016d-" (long part-number) (long seed))
                          StandardCharsets/UTF_8)
        out (byte-array size)
        fill (unchecked-byte (if (= 1 (long part-number)) 0x52 0x53))
        prefix-len (min (alength prefix) size)]
    (System/arraycopy prefix 0 out 0 prefix-len)
    (java.util.Arrays/fill out prefix-len size fill)
    out))

(defn normalize-etag
  "Strip optional surrounding double quotes from an S3 ETag header."
  [etag]
  (when etag
    (-> etag str/trim (str/replace #"^\"|\"$" ""))))

(defn classify-http-status
  "Classify an S3 HTTP status code and error code into a Jepsen outcome class.
  Returns one of :not-found, :precondition, :definite-fail, or :indeterminate.

  501 NotImplemented is a definite rejection: RustFS returns it for unsupported
  conditional forms (for example If-None-Match with an ETag instead of `*`),
  and the write is not applied. 500/503 remain indeterminate because the
  server may have committed before failing the response."
  [status error-code]
  (cond
    (or (= status 404)
        (= error-code "NoSuchKey")
        (= error-code "NoSuchBucket")
        (= error-code "NoSuchVersion")
        (= error-code "NoSuchUpload"))
    :not-found

    (or (= status 412)
        (= status 409)
        (= error-code "PreconditionFailed")
        (= error-code "ConditionalRequestConflict"))
    :precondition

    ;; 501 is 5xx but the S3 API uses it for an explicit, pre-commit rejection.
    (or (= status 501)
        (= error-code "NotImplemented")
        (= error-code "MethodNotAllowed")
        (and status (<= 400 status 499) (not= status 408) (not= status 429)))
    :definite-fail

    :else
    :indeterminate))

(defn classify-exception
  "Classify a thrown AWS SDK / Java I/O exception for Jepsen :fail vs :info."
  [^Throwable e]
  (cond
    (instance? NoSuchKeyException e)
    {:class :not-found :status 404 :code "NoSuchKey" :message (.getMessage e)}

    (instance? S3Exception e)
    (let [^S3Exception s3e e
          status (.statusCode s3e)
          code (some-> (.awsErrorDetails s3e) (.errorCode))
          cls (classify-http-status status code)]
      {:class cls
       :status status
       :code code
       :message (.getMessage s3e)})

    (instance? SdkClientException e)
    {:class :indeterminate :status nil :code "SdkClientException" :message (.getMessage e)}

    :else
    {:class :indeterminate
     :status nil
     :code (.getSimpleName (class e))
     :message (.getMessage e)}))

(defn make-client
  "Build a path-style AWS SDK v2 S3Client pointed at `endpoint`."
  [{:keys [endpoint access-key secret-key region timeout-ms]
    :or {region "us-east-1" timeout-ms 5000}}]
  (let [timeout (Duration/ofMillis timeout-ms)
        http-client (-> (UrlConnectionHttpClient/builder)
                        (.connectionTimeout timeout)
                        (.socketTimeout timeout)
                        (.build))
        creds (StaticCredentialsProvider/create
               (AwsBasicCredentials/create access-key secret-key))
        s3-cfg (-> (S3Configuration/builder)
                   (.pathStyleAccessEnabled true)
                   (.chunkedEncodingEnabled false)
                   (.build))]
    (-> (S3Client/builder)
        (.endpointOverride (URI/create endpoint))
        (.region (Region/of region))
        (.credentialsProvider creds)
        (.serviceConfiguration s3-cfg)
        (.httpClient http-client)
        (.build))))

(defn close-client!
  [^S3Client client]
  (when client
    (try (.close client) (catch Throwable _))))

(defn ensure-bucket!
  "Idempotently create `bucket` and optionally enable S3 versioning."
  [^S3Client client bucket & {:keys [versioned?] :or {versioned? false}}]
  (try
    (.headBucket client (-> (HeadBucketRequest/builder) (.bucket bucket) (.build)))
    (catch S3Exception _
      (try
        (.createBucket client (-> (CreateBucketRequest/builder) (.bucket bucket) (.build)))
        (catch S3Exception e
          (let [code (some-> (.awsErrorDetails e) (.errorCode))]
            (when-not (contains? #{"BucketAlreadyOwnedByYou" "BucketAlreadyExists"} code)
              (throw e)))))))
  (when versioned?
    (.putBucketVersioning
     client
     (-> (PutBucketVersioningRequest/builder)
         (.bucket bucket)
         (.versioningConfiguration
          (-> (VersioningConfiguration/builder)
              (.status BucketVersioningStatus/ENABLED)
              (.build)))
         (.build))))
  :ok)

(defn put-object!
  "Put UTF-8 string or byte-array `body` at `bucket`/`key` with optional
  `if-match` or `if-none-match` conditional headers."
  [^S3Client client bucket key body & {:keys [if-match if-none-match]}]
  (let [bytes (if (bytes? body)
                body
                (.getBytes (str body) StandardCharsets/UTF_8))
        b (-> (PutObjectRequest/builder)
              (.bucket bucket)
              (.key key))
        _ (when if-match (.ifMatch b if-match))
        _ (when if-none-match (.ifNoneMatch b if-none-match))
        resp (.putObject client (.build b) (RequestBody/fromBytes bytes))]
    {:etag (normalize-etag (.eTag resp))
     :version-id (.versionId resp)
     :sha256 (sha256-hex bytes)}))

(defn get-object!
  "Read object at `bucket`/`key` (optionally specific `version-id`).
  Returns {:body string :bytes byte-array :etag str :version-id str :sha256 str}."
  [^S3Client client bucket key & {:keys [version-id]}]
  (let [b (-> (GetObjectRequest/builder)
              (.bucket bucket)
              (.key key))
        _ (when version-id (.versionId b version-id))
        resp-bytes (.getObjectAsBytes client (.build b))
        raw (.asByteArray resp-bytes)
        resp (.response resp-bytes)]
    {:body (String. raw StandardCharsets/UTF_8)
     :bytes raw
     :etag (normalize-etag (.eTag resp))
     :version-id (.versionId resp)
     :sha256 (sha256-hex raw)}))

(defn delete-object!
  "Delete `bucket`/`key` (optionally specific `version-id`)."
  [^S3Client client bucket key & {:keys [version-id]}]
  (let [b (-> (DeleteObjectRequest/builder)
              (.bucket bucket)
              (.key key))
        _ (when version-id (.versionId b version-id))
        resp (.deleteObject client (.build b))]
    {:version-id (.versionId resp)
     :delete-marker? (boolean (.deleteMarker resp))}))

(defn list-objects!
  "List all keys in `bucket` (optional `prefix`) using paginated ListObjectsV2."
  [^S3Client client bucket & {:keys [prefix]}]
  (loop [token nil
         acc []]
    (let [b (-> (ListObjectsV2Request/builder)
                (.bucket bucket))
          _ (when prefix (.prefix b prefix))
          _ (when token (.continuationToken b token))
          resp (.listObjectsV2 client (.build b))
          page (mapv (fn [obj]
                       {:key (.key obj)
                        :size (.size obj)
                        :etag (normalize-etag (.eTag obj))})
                     (.contents resp))
          acc' (into acc page)]
      (if (.isTruncated resp)
        (recur (.nextContinuationToken resp) acc')
        acc'))))

(defn- version-entry
  [v]
  {:key (.key v)
   :version-id (.versionId v)
   :latest? (boolean (.isLatest v))
   :etag (normalize-etag (.eTag v))})

(defn- delete-marker-entry
  [dm]
  {:key (.key dm)
   :version-id (.versionId dm)
   :latest? (boolean (.isLatest dm))})

(defn list-versions!
  "List every object version and delete marker in `bucket` (optional `prefix`).

  ListObjectVersions returns at most 1,000 entries per response. A single page
  drops acknowledged versions once a versioned key is overwritten enough times,
  which the durability checker would report as data loss."
  [^S3Client client bucket & {:keys [prefix]}]
  (loop [key-marker nil
         version-marker nil
         versions []
         delete-markers []]
    (let [b (-> (ListObjectVersionsRequest/builder)
                (.bucket bucket)
                (.maxKeys (int 1000)))
          _ (when prefix (.prefix b prefix))
          _ (when key-marker (.keyMarker b key-marker))
          _ (when version-marker (.versionIdMarker b version-marker))
          resp (.listObjectVersions client (.build b))
          versions' (into versions (mapv version-entry (.versions resp)))
          markers' (into delete-markers (mapv delete-marker-entry (.deleteMarkers resp)))]
      (if (.isTruncated resp)
        (let [next-key (.nextKeyMarker resp)]
          (when (str/blank? next-key)
            (throw (ex-info "ListObjectVersions was truncated without a next key marker."
                            {:prefix prefix :listed (count versions')})))
          (recur next-key (.nextVersionIdMarker resp) versions' markers'))
        {:versions versions'
         :delete-markers markers'}))))

(defn create-multipart!
  [^S3Client client bucket key]
  (let [resp (.createMultipartUpload
              client
              (-> (CreateMultipartUploadRequest/builder)
                  (.bucket bucket)
                  (.key key)
                  (.build)))]
    (.uploadId resp)))

(defn upload-part!
  [^S3Client client bucket key upload-id part-number ^bytes data]
  (let [resp (.uploadPart
              client
              (-> (UploadPartRequest/builder)
                  (.bucket bucket)
                  (.key key)
                  (.uploadId upload-id)
                  (.partNumber (int part-number))
                  (.build))
              (RequestBody/fromBytes data))]
    {:part-number part-number
     :etag (.eTag resp)}))

(defn complete-multipart!
  [^S3Client client bucket key upload-id parts]
  (let [completed-parts (mapv (fn [{:keys [part-number etag]}]
                                (-> (CompletedPart/builder)
                                    (.partNumber (int part-number))
                                    (.eTag etag)
                                    (.build)))
                              (sort-by :part-number parts))
        resp (.completeMultipartUpload
              client
              (-> (CompleteMultipartUploadRequest/builder)
                  (.bucket bucket)
                  (.key key)
                  (.uploadId upload-id)
                  (.multipartUpload
                   (-> (CompletedMultipartUpload/builder)
                       (.parts completed-parts)
                       (.build)))
                  (.build)))]
    {:etag (normalize-etag (.eTag resp))
     :version-id (.versionId resp)}))

(defn abort-multipart!
  [^S3Client client bucket key upload-id]
  (.abortMultipartUpload
   client
   (-> (AbortMultipartUploadRequest/builder)
       (.bucket bucket)
       (.key key)
       (.uploadId upload-id)
       (.build)))
  :aborted)
