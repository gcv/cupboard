(ns test.test-all
  (:gen-class)
  (:use clojure.contrib.test-is)
  (:require [test.cupboard core marshal utils]
            [test.cupboard.db bdb-je]))


(defn -main [& args]
  (apply run-tests
         (map find-ns ['test.cupboard.utils
                       'test.cupboard.marshal
                       'test.cupboard.db.bdb-je
                       'test.cupboard.core]))
  (System/exit 0))
