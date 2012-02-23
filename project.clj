(defproject cupboard "1.0.0-SNAPSHOT"
  :description "An embedded database library for Clojure."
  :repositories {"oracle" "http://download.oracle.com/maven"}
  :dependencies [[org.clojure/clojure "1.3.0"]

                 [com.sleepycat/je "4.0.92"]
                 [joda-time "1.6.2"]]
  :dev-dependencies [[swank-clojure "1.2.1"]])
