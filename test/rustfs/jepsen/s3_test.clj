(ns rustfs.jepsen.s3-test
  (:require [clojure.test :refer [deftest is testing]]
            [rustfs.jepsen.s3 :as s3]))

(deftest error-classification-test
  (testing "404 and NoSuchKey map to :not-found"
    (is (= :not-found (s3/classify-http-status 404 "NoSuchKey")))
    (is (= :not-found (s3/classify-http-status 404 nil))))

  (testing "412 and 409 map to :precondition"
    (is (= :precondition (s3/classify-http-status 412 "PreconditionFailed")))
    (is (= :precondition (s3/classify-http-status 409 "ConditionalRequestConflict"))))

  (testing "400/403/501 map to :definite-fail while 500/503/408/429 map to :indeterminate"
    (is (= :definite-fail (s3/classify-http-status 400 "InvalidArgument")))
    (is (= :definite-fail (s3/classify-http-status 403 "AccessDenied")))
    (is (= :definite-fail (s3/classify-http-status 405 "MethodNotAllowed")))
    (is (= :definite-fail (s3/classify-http-status 501 "NotImplemented")))
    (is (= :definite-fail (s3/classify-http-status nil "NotImplemented")))
    (is (= :indeterminate (s3/classify-http-status 503 "SlowDown")))
    (is (= :indeterminate (s3/classify-http-status 500 "InternalError")))
    (is (= :indeterminate (s3/classify-http-status 408 "RequestTimeout")))
    (is (= :indeterminate (s3/classify-http-status nil nil)))))

(deftest helpers-test
  (is (= "abc123" (s3/normalize-etag "\"abc123\"")))
  (is (= "abc123" (s3/normalize-etag "abc123")))
  (is (= 64 (count (s3/sha256-hex "rustfs-jepsen")))))

(deftest multipart-part-size-test
  (testing "non-final parts stay at least 5 MiB after the seed prefix is applied"
    (let [part (s3/multipart-part-bytes 7 1 s3/s3-min-part-size)
          tail (s3/multipart-part-bytes 7 2 1024)]
      (is (= s3/s3-min-part-size (alength part)))
      (is (= 1024 (alength tail)))
      (is (= (s3/sha256-hex-chunks [part tail])
             (s3/sha256-hex (byte-array (concat (seq part) (seq tail)))))))))
