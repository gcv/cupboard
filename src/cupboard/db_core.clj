(ns cupboard.db-core
  (:use [cupboard.marshal])
  (:require [clojure.contrib.java-utils :as c.c.java-utils])
  (:import [com.sleepycat.je DatabaseException DatabaseEntry LockMode OperationStatus])
  (:import [com.sleepycat.je EnvironmentConfig Environment])
  (:import [com.sleepycat.je Database DatabaseConfig])
  (:import [com.sleepycat.je Cursor CursorConfig])
  (:import [com.sleepycat.je SecondaryDatabase SecondaryConfig SecondaryKeyCreator]))



;;; ----------------------------------------------------------------------
;;; database environments
;;; ----------------------------------------------------------------------

(defstruct db-env
  :dir
  :conf
  :env-handle)


;; TODO: Error handling?
(defn db-env-open [dir & conf-args]
  (let [defaults {:allow-create  false
                  :read-only     false
                  :transactional false}
        dir      (c.c.java-utils/file dir)
        conf     (merge defaults (apply hash-map conf-args))
        conf-obj (doto (EnvironmentConfig.)
                   (.setAllowCreate   (conf :allow-create))
                   (.setReadOnly      (conf :read-only))
                   (.setTransactional (conf :transactional)))]
    (when-not (.exists dir) (.mkdir dir)) ; TODO: mkdir -p in Java?
    (struct-map db-env
      :dir  dir
      :conf conf
      :env-handle (Environment. dir conf-obj))))


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



;;; ----------------------------------------------------------------------
;;; primary databases
;;; ----------------------------------------------------------------------

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
;; TODO: Add support for setting :btree-comparator and :duplicate-comparator
;; TODO: Add support for overriding :btree-comparator and :duplicate-comparator
;; (DatabaseConfig.setOverrideBtreeCompatator(), etc.)
(defn db-open [db-env name & conf-args]
  (let [defaults {:allow-create      false
                  :deferred-write    false
                  :temporary         false
                  :sorted-duplicates false
                  :exclusive-create  false
                  :read-only         false
                  :transactional     false}
        conf     (merge defaults (apply hash-map conf-args))
        conf-obj (doto (DatabaseConfig.)
                   (.setAllowCreate      (conf :allow-create))
                   (.setDeferredWrite    (conf :deferred-write))
                   (.setSortedDuplicates (conf :sorted-duplicates))
                   (.setExclusiveCreate  (conf :exclusive-create))
                   (.setReadOnly         (conf :read-only))
                   (.setTransactional    (conf :transactional)))]
    (struct-map db
      :env  db-env
      :name name
      :conf conf
      :db-handle (.openDatabase
                  (db-env :env-handle) nil name conf-obj))))


;; TODO: Convenience with-db macro


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
(defn db-put [db key data & opts-args]
  (let [defaults   {:overwrite true
                    :dup-data  true}
        opts       (merge defaults (apply hash-map opts-args))
        key-entry  (marshal-db-entry key)
        data-entry (marshal-db-entry data)]
    (cond
      (not (opts :dup-data))  (.putNoDupData
                               (db :db-handle)
                               nil key-entry data-entry)
      (not (opts :overwrite)) (.putNoOverwrite
                               (db :db-handle)
                               nil key-entry data-entry)
      :else (.put (db :db-handle) nil key-entry data-entry))))


;; TODO: Error handling?
;; TODO: This should return a status if entry not found, or something similar!
(defn db-get [db key & opts-args]
  (let [defaults   {:data      nil
                    :lock-mode LockMode/DEFAULT}
        opts       (merge defaults (apply hash-map opts-args))
        key-entry  (marshal-db-entry key)
        data-entry (marshal-db-entry (opts :data))]
    (if (not (nil? (opts :data)))
        (.getSearchBoth (db :db-handle) nil key-entry data-entry (opts :lock-mode))
        (.get (db :db-handle) nil key-entry data-entry (opts :lock-mode)))
    [(unmarshal-db-entry key-entry) (unmarshal-db-entry data-entry)]))


;; TODO: Error handling?
;; TODO: This should return a status or return code.
(defn db-delete [db key]
  (let [key-entry (marshal-db-entry key)]
    (.delete (db :db-handle) nil key-entry)))



;;; ----------------------------------------------------------------------
;;; primary database cursors
;;; ----------------------------------------------------------------------

(defstruct db-cursor
  :db
  :conf
  :db-cursor-handle)


;; TODO: Error handling?
;; TODO: Track cursors in the db struct!!! Clean them up when done!!!
(defn db-cursor-open [db & conf-args]
  (let [defaults {:read-committed   true
                  :read-uncommitted false}
        conf     (merge defaults (apply hash-map conf-args))
        conf-obj (doto (CursorConfig.)
                   (.setReadCommitted   (conf :read-committed))
                   (.setReadUncommitted (conf :read-uncommitted)))]
    (struct-map db-cursor
      :db   db
      :conf conf
      :db-cursor-handle (.openCursor (db :db-handle) nil conf-obj))))


;; TODO: Error handling?
;; TODO: REMOVE CLOSED CURSORS FROM THE db struct!!!
(defn db-cursor-close [db-cursor]
  (.close (db-cursor :db-cursor-handle)))


;; TODO: Convenience with-db-cursor macro


(defn db-cursor-get [db-cursor key & opts-args]
  (let [defaults     {:data      nil
                      :direction :forward   ; or :back
                      :exact     false
                      :skip-dups false
                      :lock-mode LockMode/DEFAULT}
        opts         (merge defaults (apply hash-map opts-args))
        direction    (opts :direction)
        exact        (opts :exact)
        skip-dups    (opts :skip-dups)
        lock-mode    (opts :lock-mode)
        key-entry    (marshal-db-entry key)
        data-entry   (marshal-db-entry (opts :data))
        search-fn    (cond (and (nil? (opts :data)) :exact)       #(.getSearchKey %1 %2 %3 %4)
                           (nil? (opts :data))                    #(.getSearchKeyRange %1 %2 %3 %4)
                           exact                                  #(.getSearchBoth %1 %2 %3 %4)
                           :else                                  #(.getSearchBothRange %1 %2 %3 %4))
        direction-fn (cond (and (= direction :forward) skip-dups) #(.getNextNoDup %1 %2 %3 %4)
                           (and (= direction :back) skip-dups)    #(.getPrevNoDup %1 %2 %3 %4)
                           (= direction :forward)                 #(.getNext %1 %2 %3 %4)
                           (= direction :back)                    #(.getPrev %1 %2 %3 %4))]
    (letfn [(cursor-seq [next-fn]
              (let [seek-res (next-fn
                              (db-cursor :db-cursor-handle)
                              key-entry data-entry lock-mode)]
                (if (= seek-res OperationStatus/SUCCESS)
                    (lazy-seq (cons [(unmarshal-db-entry key-entry)
                                     (unmarshal-db-entry data-entry)]
                                    (cursor-seq direction-fn)))
                    (lazy-seq))))]
      (cursor-seq search-fn))))
;; TODO: What about closing the cursor?


;; TODO: db-cursor-put


;; TODO: db-cursor-delete


;; TODO: Think about db-cursor-replace to overwrite the current
;; location..? Probably not possible.



;;; ----------------------------------------------------------------------
;;; secondary databases (indices)
;;; ----------------------------------------------------------------------

(defstruct db-sec
  :env
  :name
  :db
  :conf
  :db-sec-handle)


;; TODO: Error handling?
(defn db-sec-open [db-env db name & conf-args]
  ;; TODO: Make this way smoother!
  (when ((db :conf) :sorted-duplicates)
    :error-out-in-flames)
  (let [defaults    {:key-creator-fn    first
                     :allow-create      false
                     :sorted-duplicates false
                     :allow-populate    true}
        conf        (merge defaults (apply hash-map conf-args))
        key-creator (proxy [SecondaryKeyCreator] []
                      (createSecondaryKey [_ key-entry data-entry result-entry]
                        (let [data     (unmarshal-db-entry data-entry)
                              sec-data ((conf :key-creator-fn) data)]
                          (if sec-data
                              (do
                                (marshal-db-entry sec-data result-entry)
                                true)
                              false))))
        conf-obj    (doto (SecondaryConfig.)
                      (.setKeyCreator       key-creator)
                      (.setAllowCreate      (conf :allow-create))
                      (.setSortedDuplicates (conf :sorted-duplicates))
                      (.setAllowPopulate    (conf :allow-populate)))]
    (struct-map db-sec
      :env  db-env
      :db   db
      :name name
      :conf conf
      :db-sec-handle (.openSecondaryDatabase
                      (db-env :env-handle)
                      nil
                      name (db :db-handle) conf-obj))))


;; TODO: Error handling?
(defn db-sec-close [db-sec]
  ;; TODO: Deal with all open cursors on the database.
  (.close (db-sec :db-sec-handle)))


;; TODO: Convenience with-db-sec macro


;; TODO: Error handling?
(defn db-sec-get [db-sec search-key & opts-args]
  (let [defaults         {:lock-mode LockMode/DEFAULT}
        opts             (merge defaults (apply hash-map opts-args))
        search-key-entry (marshal-db-entry search-key)
        key-entry        (DatabaseEntry.)
        data-entry       (DatabaseEntry.)]
    (.get (db-sec :db-sec-handle) nil
          search-key-entry key-entry data-entry (opts :lock-mode))
    [(unmarshal-db-entry key-entry) (unmarshal-db-entry data-entry)]))


;; TODO: Error handling?
(defn db-sec-delete [db-sec search-key]
  (let [search-entry (marshal-db-entry search-key)]
    (.delete (db-sec :db-sec-handle) nil search-entry)))
