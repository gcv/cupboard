(ns cupboard.dbcore
  (:use [cupboard.marshal])
  (:require [clojure.contrib.java-utils :as c.c.java-utils])
  (:import [com.sleepycat.je DatabaseException DatabaseEntry LockMode])
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


;; TODO: db-env-sync


(defstruct db
  :env
  :name
  :conf
  :db-handle)


;; TODO: Error handling?
(defn db-close [db]
  ;; TODO: Deal with all open cursors on the database.
  (.close (db :db-handle)))


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


;; TODO: Error handling?
(defn db-sync [db]
  (when (.getDeferredWrite (db :conf))
    (.sync (db :db-handle))))


;; TODO: (defn db-preload [db & preload-conf-args] ...)


;; TODO: (defn db-remove [db-env name] ...)


;; TODO: (defn db-truncate [db-env name & truncate-conf-args] ...)
;; args: {:txn handle :count false}


;; TODO: Error handling?
;; TODO: This should return a status of some kind!
(defn rput [db key data & opts-args]
  (let [defaults   {:overwrite true
                    :dup-data  true}
        opts       (merge defaults (apply hash-map opts-args))
        key-entry  (marshal-db-entry key)
        data-entry (marshal-db-entry data)]
    (cond
      (not (opts :dup-data))  (.putNoDupData   (db :db-handle) nil key-entry data-entry)
      (not (opts :overwrite)) (.putNoOverwrite (db :db-handle) nil key-entry data-entry)
      true (.put (db :db-handle) nil key-entry data-entry))))


;; TODO: Error handling?
;; TODO: This should return a status if entry not found, or something similar!
(defn rget [db key & opts-args]
  (let [defaults   {:search-both false
                    :data        nil
                    :lock-mode   LockMode/DEFAULT}
        opts       (merge defaults (apply hash-map opts-args))
        key-entry  (marshal-db-entry key)
        data-entry (if (opts :data)
                       (marshal-db-entry (opts :data))
                       (DatabaseEntry.))]
    (if (opts :search-both)
        (.getSearchBoth (db :db-handle) nil key-entry data-entry (opts :lock-mode))
        (.get (db :db-handle) nil key-entry data-entry (opts :lock-mode)))
    (unmarshal-db-entry data-entry)))


;; TODO: Error handling?
;; TODO: This should return a status or return code.
(defn rdelete [db key & opts-args]
  ;; TODO: opts-args should have some way to distinguish between
  ;; removing all matching records, or using a cursor to delete some
  ;; of the duplicates.
  (let [key-entry (marshal-db-entry key)]
    (.delete (db :db-handle) key-entry)))
