(defproject cupboard "1.0beta1"
  :description "An embedded database library for Clojure."
  :repositories {"oracle" "http://download.oracle.com/maven"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.sleepycat/je "5.0.55"]
                 [joda-time "2.1"]]
  :dev-dependencies [[swank-clojure "1.4.0"]])
