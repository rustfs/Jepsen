(defproject rustfs-jepsen "0.1.0"
  :description "Official Jepsen distributed consistency and fault-tolerance test suite for RustFS."
  :url "https://github.com/rustfs/Jepsen"
  :license {:name "Apache License 2.0"
            :url "https://www.apache.org/licenses/LICENSE-2.0"}
  ;; AWS SDK v2 transitively references SLF4J 1.7; pin SLF4J 2.0.16 to match
  ;; Jepsen 0.3.16's Logback 1.5 runtime cleanly.
  :dependencies [[org.clojure/clojure "1.12.7"]
                 [org.slf4j/slf4j-api "2.0.16"]
                 [jepsen "0.3.16"]
                 [software.amazon.awssdk/s3 "2.29.45" :exclusions [org.slf4j/slf4j-api]]
                 [software.amazon.awssdk/url-connection-client "2.29.45" :exclusions [org.slf4j/slf4j-api]]]
  :main rustfs.jepsen.core
  :jvm-opts ["-Xmx3g"
             "-Djava.awt.headless=true"
             "-server"
             "-Djava.net.preferIPv4Stack=true"]
  :profiles {:dev {:jvm-opts ["-Xmx2g" "-Djava.awt.headless=true"]}})
