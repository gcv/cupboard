(ns leftb-storage
  (:require [clojure.contrib.java-utils :as c.c.java-utils])
  (:import [com.sleepycat.je DatabaseException])
  (:import [com.sleepycat.je EnvironmentConfig Environment])
  (:import [com.sleepycat.je DatabaseConfig Database]))


(defstruct db-env
  :dir
  :conf
  :env-handle
  :databases)


;; TODO: Error handling?
(defn db-env-open [dir & conf-args]
  (let [defaults {:allow-create  false
                  :read-only     false
                  :transactional false}
        dir      (c.c.java-utils/file dir)
        conf-map (merge defaults (apply hash-map conf-args))
        conf     (doto (EnvironmentConfig.)
                   (.setAllowCreate   (conf-map :allow-create))
                   (.setReadOnly      (conf-map :read-only))
                   (.setTransactional (conf-map :transactional)))]
    (when-not (.exists dir) (.mkdir dir)) ; TODO: mkdir -p in Java?
    (struct-map db-env
      :dir dir
      :conf conf
      :env-handle (Environment. dir conf)
      :databases (atom {}))))


;; TODO: EnvironmentMutableConfig handling


;; TODO: Error handling?
(defn db-env-close [db-env]
  ;; TODO: Close all open database handles in this environment (be
  ;; sure to use the Clojure db-close function).
  ;; TODO: Clear out the env-handle afterwards to avoid using it.
  (.cleanLog (db-env :env-handle))
  (.close (db-env :env-handle)))


;; TODO: Environment statistics gathering


;; TODO: Convenience with-db-env macro


(defstruct db
  :env
  :name
  :conf
  :db-handle)


;; TODO: Error handling?
(defn db-open [db-env name & conf-args]
  (when-let [existing-db (contains? @(db-env :databases) name)]
    ;; flush deferred-write data, close handle, remove handle from db-env
    (db-close existing-db)
    (swap! (db-env :databases) dissoc name))
  (let [defaults {:allow-create      false
                  :deferred-write    false
                  :temporary         false
                  :sorted-duplicates false
                  :exclusive-create  false
                  :read-only         false
                  :transactional     false}
        conf-map (merge defaults (apply hash-map conf-args))
        conf     (doto (DatabaseConfig.)
                   (.setAllowCreate      (conf-map :allow-create))
                   (.setDeferredWrite    (conf-map :deferred-write))
                   (.setSortedDuplicates (conf-map :sorted-duplicates))
                   (.setExclusiveCreate  (conf-map :exclusive-create))
                   (.setReadOnly         (conf-map :read-only))
                   (.setTransactional    (conf-map :transactional)))
        db       (struct-map db
                   :env db-env
                   :name name
                   :conf conf
                   :db-handle (.openDatabase
                               (db-env :env-handle) nil name conf))]
    ;; Push this database into the (db-env :databases) atom and return
    ;; the db object.
    (swap! (db-env :databases) assoc name db)
    db))


(defn db-sync [db]
  (when (.getDeferredWrite (db :conf))
    (.sync (db :db-handle))))


(defn db-close [db]
  ;; TODO: Deal with all open cursors on the database.
  (.close (db :db-handle)))
