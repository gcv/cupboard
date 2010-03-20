(ns test.test-all
  (:gen-class)
  (:use clojure.contrib.test-is)
  (:require [test.cupboard core utils]
            [test.cupboard.bdb je je-marshal]))


(defn -main [& args]
  (apply run-tests
         (map find-ns ['test.cupboard.utils
                       'test.cupboard.bdb.je-marshal
                       'test.cupboard.bdb.je
                       'test.cupboard.core]))
  (System/exit 0))
