(ns leftb-storage
  (:require [clojure.contrib.java-utils :as c.c.java-utils])
  (:import [com.sleepycat.je Environment EnvironmentConfig DatabaseException]))


(defstruct db-env
  :directory
  :configuration
  :environment
  :databases)


;; TODO: Error handling?
(defn open-db-env [dir & conf-args]
  (let [defaults {:allow-create false
                  :read-only false
                  :transactional false}
        dir      (if (instance? java.io.File dir) dir (c.c.java-utils/file dir))
        conf-map (merge defaults (apply hash-map conf-args))
        conf     (doto (EnvironmentConfig.)
                   (.setAllowCreate   (conf-map :allow-create))
                   (.setReadOnly      (conf-map :read-only))
                   (.setTransactional (conf-map :transactional)))]
    (when-not (.exists dir) (.mkdir dir)) ; TODO: mkdir -p in Java?
    (struct-map db-env
      :directory dir
      :configuration conf
      :environment (Environment. dir conf)
      :databases (atom {}))))


;; TODO: EnvironmentMutableConfig handling


;; TODO: Error handling?
(defn close-db-env [db-env]
  (.cleanLog (db-env :environment))
  (.close (db-env :environment)))


;; TODO: Environment statistics gathering


;; TODO: Convenience with-db-env macro


(defstruct db
  :environment
  :name
  :configuration)


; (defn open-db 
