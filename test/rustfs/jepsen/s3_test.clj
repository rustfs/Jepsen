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

  (testing "400/403 map to :definite-fail while 500/503/408/429 map to :indeterminate"
    (is (= :definite-fail (s3/classify-http-status 400 "InvalidArgument")))
    (is (= :definite-fail (s3/classify-http-status 403 "AccessDenied")))
    (is (= :indeterminate (s3/classify-http-status 503 "SlowDown")))
    (is (= :indeterminate (s3/classify-http-status 500 "InternalError")))
    (is (= :indeterminate (s3/classify-http-status 408 "RequestTimeout")))
    (is (= :indeterminate (s3/classify-http-status nil nil)))))

(deftest helpers-test
  (is (= "abc123" (s3/normalize-etag "\"abc123\"")))
  (is (= "abc123" (s3/normalize-etag "abc123")))
  (is (= 64 (count (s3/sha256-hex "rustfs-jepsen")))))
