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
           [software.amazon.awssdk.core.client.config ClientOverrideConfiguration]
           [software.amazon.awssdk.core.exception SdkClientException]
           [software.amazon.awssdk.core.sync RequestBody]
           [software.amazon.awssdk.http.apache ApacheHttpClient]
           [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.retries StandardRetryStrategy]
           [software.amazon.awssdk.retries.api RetryStrategy]
           [software.amazon.awssdk.services.s3 S3Client S3ClientBuilder S3Configuration]
           [software.amazon.awssdk.services.s3.model
            AbortMultipartUploadRequest
            BucketVersioningStatus
            CompleteMultipartUploadRequest
            CompletedMultipartUpload
            CompletedPart
            CopyObjectRequest
            CreateBucketRequest
            CreateMultipartUploadRequest
            DeleteObjectRequest
            DeleteObjectTaggingRequest
            GetObjectRequest
            GetObjectRetentionRequest
            GetObjectTaggingRequest
            HeadBucketRequest
            HeadObjectRequest
            ListObjectVersionsRequest
            ListObjectsV2Request
            NoSuchKeyException
            ObjectLockLegalHold
            ObjectLockLegalHoldStatus
            ObjectLockRetention
            ObjectLockRetentionMode
            PutBucketVersioningRequest
            PutObjectLegalHoldRequest
            PutObjectRequest
            PutObjectRetentionRequest
            PutObjectTaggingRequest
            RestoreObjectRequest
            RestoreRequest
            S3Exception
            Tag
            Tagging
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
  "Build a path-style S3 client that records the outcome of one request attempt."
  [{:keys [endpoint access-key secret-key region timeout-ms]
    :or {region "us-east-1" timeout-ms 5000}}]
  (let [timeout (Duration/ofMillis timeout-ms)
        ;; The Apache transport also disables HTTP-level retries. HttpURLConnection
        ;; can replay a request when the connection closes before response headers.
        http-client (-> (ApacheHttpClient/builder)
                        (.connectionTimeout timeout)
                        (.socketTimeout timeout)
                        (.connectionAcquisitionTimeout timeout))
        ;; A successful mutation followed by a lost response must stay indeterminate
        ;; even when retrying it would result in a definite rejection.
        retry-strategy (-> (StandardRetryStrategy/builder) (.maxAttempts 1) (.build))
        client-config (-> (ClientOverrideConfiguration/builder)
                          (.retryStrategy ^RetryStrategy retry-strategy)
                          (.build))
        creds (StaticCredentialsProvider/create
               (AwsBasicCredentials/create access-key secret-key))
        s3-cfg (-> (S3Configuration/builder)
                   (.pathStyleAccessEnabled true)
                   (.chunkedEncodingEnabled false)
                   (.build))
        ^S3ClientBuilder builder (S3Client/builder)]
    (.endpointOverride builder (URI/create endpoint))
    (.region builder (Region/of region))
    (.credentialsProvider builder creds)
    (.serviceConfiguration builder ^S3Configuration s3-cfg)
    (.overrideConfiguration builder ^ClientOverrideConfiguration client-config)
    (.httpClientBuilder builder http-client)
    (.build builder)))

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
         seen-tokens #{}
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
        (let [next-token (.nextContinuationToken resp)]
          (when (or (str/blank? next-token) (contains? seen-tokens next-token))
            (throw (ex-info "ListObjectsV2 returned a missing or repeated continuation token."
                            {:bucket bucket :prefix prefix :listed (count acc')})))
          (recur next-token (conj seen-tokens next-token) acc'))
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
         seen-markers #{}
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
        (let [next-key (.nextKeyMarker resp)
              next-version (.nextVersionIdMarker resp)
              next-marker [next-key next-version]]
          (when (or (str/blank? next-key) (contains? seen-markers next-marker))
            (throw (ex-info "ListObjectVersions returned a missing or repeated marker."
                            {:bucket bucket :prefix prefix
                             :listed (+ (count versions') (count markers'))})))
          (recur next-key next-version (conj seen-markers next-marker) versions' markers'))
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

(defn head-object!
  "HEAD an object returning metadata, storage class, replication & restore headers."
  [^S3Client client bucket key & {:keys [version-id]}]
  (let [b (-> (HeadObjectRequest/builder) (.bucket bucket) (.key key))
        _ (when version-id (.versionId b version-id))
        resp (.headObject client (.build b))]
    {:etag (normalize-etag (.eTag resp))
     :version-id (.versionId resp)
     :content-length (.contentLength resp)
     :storage-class (.storageClassAsString resp)
     :replication-status (.replicationStatusAsString resp)
     :restore (.restore resp)
     :last-modified (some-> (.lastModified resp) (.toEpochMilli))}))

(defn copy-object!
  "Server-side CopyObject from `src-bucket/src-key` to `dst-bucket/dst-key`."
  [^S3Client client src-bucket src-key dst-bucket dst-key]
  (let [resp (.copyObject
              client
              (-> (CopyObjectRequest/builder)
                  (.sourceBucket src-bucket)
                  (.sourceKey src-key)
                  (.destinationBucket dst-bucket)
                  (.destinationKey dst-key)
                  (.build)))]
    {:etag (some-> (.copyObjectResult resp) (.eTag) normalize-etag)
     :version-id (.versionId resp)}))

(defn put-object-retention!
  "Apply WORM Object Lock retention (`:compliance` or `:governance`) until `retain-until-instant`."
  [^S3Client client bucket key mode retain-until-instant & {:keys [version-id bypass-governance?]}]
  (let [sdk-mode (if (= mode :compliance)
                   ObjectLockRetentionMode/COMPLIANCE
                   ObjectLockRetentionMode/GOVERNANCE)
        b (-> (PutObjectRetentionRequest/builder)
              (.bucket bucket)
              (.key key)
              (.retention (-> (ObjectLockRetention/builder)
                              (.mode sdk-mode)
                              (.retainUntilDate retain-until-instant)
                              (.build))))
        _ (when version-id (.versionId b version-id))
        _ (when bypass-governance? (.bypassGovernanceRetention b true))]
    (.putObjectRetention client (.build b))
    :ok))

(defn put-object-legal-hold!
  "Toggle S3 Object Lock Legal Hold (`true` -> ON, `false` -> OFF)."
  [^S3Client client bucket key on? & {:keys [version-id]}]
  (let [b (-> (PutObjectLegalHoldRequest/builder)
              (.bucket bucket)
              (.key key)
              (.legalHold (-> (ObjectLockLegalHold/builder)
                              (.status (if on?
                                         ObjectLockLegalHoldStatus/ON
                                         ObjectLockLegalHoldStatus/OFF))
                              (.build))))
        _ (when version-id (.versionId b version-id))]
    (.putObjectLegalHold client (.build b))
    :ok))

(defn delete-object-with-bypass!
  "Delete an object version optionally passing `x-amz-bypass-governance-retention`."
  [^S3Client client bucket key & {:keys [version-id bypass-governance?]}]
  (let [b (-> (DeleteObjectRequest/builder) (.bucket bucket) (.key key))
        _ (when version-id (.versionId b version-id))
        _ (when bypass-governance? (.bypassGovernanceRetention b true))
        resp (.deleteObject client (.build b))]
    {:version-id (.versionId resp)
     :delete-marker? (boolean (.deleteMarker resp))}))

(defn put-object-tagging!
  "Set S3 tags map (`{\"env\" \"ephemeral\"}`) on an object."
  [^S3Client client bucket key tags-map & {:keys [version-id]}]
  (let [tag-objs (mapv (fn [[k v]]
                         (-> (Tag/builder) (.key (str k)) (.value (str v)) (.build)))
                       tags-map)
        b (-> (PutObjectTaggingRequest/builder)
              (.bucket bucket)
              (.key key)
              (.tagging (-> (Tagging/builder) (.tagSet tag-objs) (.build))))
        _ (when version-id (.versionId b version-id))]
    (.putObjectTagging client (.build b))
    :ok))

(defn delete-object-tagging!
  [^S3Client client bucket key & {:keys [version-id]}]
  (let [b (-> (DeleteObjectTaggingRequest/builder) (.bucket bucket) (.key key))
        _ (when version-id (.versionId b version-id))]
    (.deleteObjectTagging client (.build b))
    :ok))

(defn get-object-tagging!
  [^S3Client client bucket key & {:keys [version-id]}]
  (let [b (-> (GetObjectTaggingRequest/builder) (.bucket bucket) (.key key))
        _ (when version-id (.versionId b version-id))
        resp (.getObjectTagging client (.build b))]
    (into {} (map (fn [^Tag t] [(.key t) (.value t)]) (.tagSet resp)))))

(defn restore-object!
  "Initiate S3 RestoreObject for an archived/tiered object for `days` days."
  [^S3Client client bucket key days & {:keys [version-id]}]
  (let [b (-> (RestoreObjectRequest/builder)
              (.bucket bucket)
              (.key key)
              (.restoreRequest (-> (RestoreRequest/builder)
                                   (.days (int days))
                                   (.build))))
        _ (when version-id (.versionId b version-id))]
    (.restoreObject client (.build b))
    :ok))
