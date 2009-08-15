(ns cupboard.db-core
  (:use [cupboard.marshal])
  (:require [clojure.contrib.java-utils :as c.c.java-utils])
  (:import [com.sleepycat.je DatabaseException DatabaseEntry LockMode])
  (:import [com.sleepycat.je EnvironmentConfig Environment])
  (:import [com.sleepycat.je Database DatabaseConfig])
  (:import [com.sleepycat.je Cursor SecondaryCursor JoinCursor CursorConfig JoinConfig])
  (:import [com.sleepycat.je SecondaryDatabase SecondaryConfig SecondaryKeyCreator]))



;;; ----------------------------------------------------------------------
;;; useful structs
;;; ----------------------------------------------------------------------

(defstruct db-env
  :dir
  :conf
  :env-handle)


(defstruct db
  :name
  :conf
  :db-handle)


(defstruct db-sec
  :name
  :db
  :conf
  :db-sec-handle)


(defstruct db-cursor
  :db
  :conf
  :cursor-handle)


(defstruct db-join-cursor
  :db
  :conf
  :cursors
  :join-cursor-handle)


(defn db-env?
  "Returns true if the given struct represents a database environment."
  [s]
  (contains? s :env-handle))


(defn db?
  "Returns true if the given struct represents a primary database."
  [s]
  (contains? s :db-handle))


(defn db-sec?
  "Returns true if the given struct represents a secondary database."
  [s]
  (contains? s :db-sec-handle))


(defn db-cursor?
  "Returns true if the given struct represents a database cursor."
  [s]
  (contains? s :cursor-handle))


(defn db-cursor-primary?
  "Returns true if the given struct represents a primary database cursor."
  [s]
  (db? (s :db)))


(defn db-cursor-sec?
  "Returns true if the given struct represents a secondary database cursor."
  [s]
  (db-sec? (s :db)))


(defn db-join-cursor?
  "Returns true if the given struct represents a join cursor."
  [s]
  (contains? s :join-cursor-handle))



;;; ----------------------------------------------------------------------
;;; database environments
;;; ----------------------------------------------------------------------

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
    (when-not (.exists dir) (.mkdir dir))
    (struct-map db-env
      :dir  dir
      :conf conf
      :env-handle (Environment. dir conf-obj))))


;; TODO: EnvironmentMutableConfig handling


(defn db-env-close [db-env]
  ;; TODO: Close all open database handles in this environment (be
  ;; sure to use the Clojure db-close function).
  ;; TODO: Clear out the env-handle afterwards to avoid using it.
  (.cleanLog (db-env :env-handle))
  (.close (db-env :env-handle)))


;; TODO: Environment statistics gathering


;; TODO: Convenience with-db-env macro


(defn db-env-sync [db-env]
  (.sync (db-env :env-handle)))



;;; ----------------------------------------------------------------------
;;; primary databases
;;; ----------------------------------------------------------------------

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
(defn db-put
  "Optional keyword arguments:
     :no-dup-data  --- if true, then calls .putNoDupData
     :no-overwrite --- if true, then calls .putNoOverwrite"
  [db key data & opts-args]
  (let [opts       (apply hash-map opts-args)
        key-entry  (marshal-db-entry key)
        data-entry (marshal-db-entry data)]
    (when (and (opts :no-dup-data) (opts :no-overwrite))
      :flame-out) ; TODO: Implement this
    (cond (opts :no-dup-data)  (.putNoDupData
                                (db :db-handle) nil key-entry data-entry)
          (opts :no-overwrite) (.putNoOverwrite
                                (db :db-handle) nil key-entry data-entry)
          :else (.put (db :db-handle) nil key-entry data-entry))))


;; TODO: Error handling?
(defn db-get
  "Optional keyword arguments:
     :data --- if specified, searches by both exact match of both key and :data value
     :lock-mode"
  [db key & opts-args]
  (let [defaults   {:lock-mode LockMode/DEFAULT}
        opts       (merge defaults (apply hash-map opts-args))
        key-entry  (marshal-db-entry key)
        data-entry (marshal-db-entry* opts :data)
        result     (if (contains? opts :data)
                       (.getSearchBoth (db :db-handle) nil
                                       key-entry data-entry (opts :lock-mode))
                       (.get (db :db-handle) nil
                             key-entry data-entry (opts :lock-mode)))]
    (unmarshal-db-entry* result key-entry data-entry)))


;; TODO: Error handling?
;; TODO: This should return a status or return code.
(defn db-delete [db key]
  (let [key-entry (marshal-db-entry key)]
    (.delete (db :db-handle) nil key-entry)))



;;; ----------------------------------------------------------------------
;;; secondary databases (indices)
;;; ----------------------------------------------------------------------

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
        data-entry       (DatabaseEntry.)
        result           (.get (db-sec :db-sec-handle) nil
                               search-key-entry key-entry data-entry
                               (opts :lock-mode))]
    (unmarshal-db-entry* result key-entry data-entry)))


;; TODO: Error handling?
(defn db-sec-delete [db-sec search-key]
  (let [search-entry (marshal-db-entry search-key)]
    (.delete (db-sec :db-sec-handle) nil search-entry)))



;;; ----------------------------------------------------------------------
;;; database cursors
;;;
;;; This code supports both primary and secondary cursors, and treats them
;;; differently only where absolutely necessary. Calling code should simply
;;; pass the appropriate database into db-cursor-open.
;;; ----------------------------------------------------------------------

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
      :cursor-handle (if (db? db)
                         (.openCursor (db :db-handle) nil conf-obj)
                         (.openSecondaryCursor (db :db-sec-handle) nil conf-obj)))))


;; TODO: Error handling?
;; TODO: REMOVE CLOSED CURSORS FROM THE db struct!!!
(defn db-cursor-close [db-cursor]
  (.close (db-cursor :cursor-handle)))


;; TODO: Convenience with-db-cursor macro


;; TODO: Error handling?
;; TODO: Write tests to check "both" search mode.
(defn db-cursor-search
  "Optional keyword arguments:
     :pkey  --- for cursors on secondary databases only, specifies the primary key value
     :data  --- if specified, positions the cursor by both key and :data values
     :exact --- if true, match the key and optional :data exactly"
  [db-cursor key & opts-args]
  (let [defaults   {:exact     false
                    :lock-mode LockMode/DEFAULT}
        opts       (merge defaults (apply hash-map opts-args))
        exact      (opts :exact)
        key-entry  (marshal-db-entry key)
        pkey-entry (when (db-cursor-sec? db-cursor) (marshal-db-entry* opts :pkey))
        data-entry (marshal-db-entry* opts :data)
        ;; search-fn1 is for primary database cursor lookups
        search-fn1 (cond
                     (and (contains? opts :data) :exact) #(.getSearchBoth %1 %2 %3 %4)
                     (contains? opts :data)              #(.getSearchBothRange %1 %2 %3 %4)
                     exact                               #(.getSearchKey %1 %2 %3 %4)
                     :else                               #(.getSearchKeyRange %1 %2 %3 %4))
        ;; search-fn2 is for secondary database cursor lookups
        search-fn2 (cond
                     (and (contains? opts :pkey) :exact) #(.getSearchBoth %1 %2 %3 %4 %5)
                     (contains? opts :pkey)              #(.getSearchBothRange %1 %2 %3 %4 %5)
                     exact                               #(.getSearchKey %1 %2 %3 %4 %5)
                     :else                               #(.getSearchKeyRange %1 %2 %3 %4 %5))
        result     (if (db-cursor-primary? db-cursor)
                       (search-fn1 (db-cursor :cursor-handle)
                                   key-entry data-entry (opts :lock-mode))
                       (search-fn2 (db-cursor :cursor-handle)
                                   key-entry pkey-entry data-entry (opts :lock-mode)))]
    (unmarshal-db-entry* result
                         (if (db-cursor-primary? db-cursor) key-entry pkey-entry)
                         data-entry)))


;; TODO: Error handling?
(defmacro def-db-cursor-simple-position [name java-fn]
  `(defn ~name
     "Optional keyword arguments:
        :pkey --- for cursors on secondary databases only, specifies the primary key value
        :key  --- if specified, reuses the given DatabaseEntry
        :data --- if specified, reuses the given DatabaseEntry"
     [db-cursor# & opts-args#]
     (let [defaults#   {:lock-mode LockMode/DEFAULT}
           opts#       (merge defaults# (apply hash-map opts-args#))
           key-entry#  (marshal-db-entry* opts# :key)
           pkey-entry# (when (db-cursor-sec? db-cursor#) (marshal-db-entry* opts# :pkey))
           data-entry# (marshal-db-entry* opts# :data)
           result#     (if (db-cursor-primary? db-cursor#)
                           (~java-fn (db-cursor# :cursor-handle)
                                     key-entry# data-entry# (opts# :lock-mode))
                           (~java-fn (db-cursor# :cursor-handle)
                                     key-entry# pkey-entry# data-entry# (opts# :lock-mode)))]
       (unmarshal-db-entry* result#
                            (if (db-cursor-primary? db-cursor#) key-entry# pkey-entry#)
                            data-entry#))))

(def-db-cursor-simple-position db-cursor-first .getFirst)
(def-db-cursor-simple-position db-cursor-current .getCurrent)
(def-db-cursor-simple-position db-cursor-last .getLast)


;; TODO: Error handling?
(defn db-cursor-next
  "Optional keyword arguments:
     :key  --- if specified, reuses the given DatabaseEntry
     :data --- if specified, reuses the given DatabaseEntry"
  [db-cursor & opts-args]
  (let [defaults   {:direction :forward
                    :skip-dups false
                    :lock-mode LockMode/DEFAULT}
        opts       (merge defaults (apply hash-map opts-args))
        direction  (opts :direction)
        skip-dups  (opts :skip-dups)
        key-entry  (marshal-db-entry* opts :key)
        pkey-entry (when (db-cursor-sec? db-cursor) (marshal-db-entry* opts :pkey))
        data-entry (marshal-db-entry* opts :data)
        ;; next-fn1 is for primary database cursors
        next-fn1   (cond
                     (and (= direction :forward) skip-dups) #(.getNextNoDup %1 %2 %3 %4)
                     (and (= direction :back) skip-dups)    #(.getPrevNoDup %1 %2 %3 %4)
                     (= direction :forward)                 #(.getNext %1 %2 %3 %4)
                     (= direction :back)                    #(.getPrev %1 %2 %3 %4))
        ;; next-fn2 is for secondary database cursors
        next-fn2   (cond
                     (and (= direction :forward) skip-dups) #(.getNextNoDup %1 %2 %3 %4 %5)
                     (and (= direction :back) skip-dups)    #(.getPrevNoDup %1 %2 %3 %4 %5)
                     (= direction :forward)                 #(.getNext %1 %2 %3 %4 %5)
                     (= direction :back)                    #(.getPrev %1 %2 %3 %4 %5))
        result     (if (db-cursor-primary? db-cursor)
                       (next-fn1 (db-cursor :cursor-handle)
                                 key-entry data-entry (opts :lock-mode))
                       (next-fn2 (db-cursor :cursor-handle)
                                 key-entry pkey-entry data-entry (opts :lock-mode)))]
    (unmarshal-db-entry* result
                         (if (db-cursor-primary? db-cursor) key-entry pkey-entry)
                         data-entry)))


;; TODO: Error handling?
(defn db-cursor-put [db-cursor key data & opts-args]
  (when (db-cursor-sec? db-cursor)
    :flame-out) ; TODO: Implement this
  (let [opts       (apply hash-map opts-args)
        key-entry  (marshal-db-entry key)
        data-entry (marshal-db-entry data)]
    (when (and (opts :no-dup-data) (opts :no-overwrite))
      :flame-out) ; TODO: Implement this
    (cond (opts :no-dup-data)  (.putNoDupData
                                (db-cursor :cursor-handle) key-entry data-entry)
          (opts :no-overwrite) (.putNoOverwrite
                                (db-cursor :cursor-handle) key-entry data-entry)
          :else (.put (db-cursor :cursor-handle) key-entry data-entry))))


;; TODO: Error handling?
(defn db-cursor-delete
  "Deletes the record the cursor currently points to."
  [db-cursor]
  (.delete (db-cursor :cursor-handle)))


;; TODO: Error handling?
(defn db-cursor-replace
  "Replaces the data entry of the record the cursor currently points to."
  [db-cursor new-data]
  (when (db-cursor-sec? db-cursor)
    :flame-out) ; TODO: Implement this
  (.putCurrent (db-cursor :cursor-handle) (marshal-db-entry new-data)))


;; TODO: Write functions to manipulate the cursor's cache mode


;; TODO: Error handling?
(defn db-join-cursor-open [db-cursors & conf-args]
  ;; check that all cursors refer to the same database
  (when-not (= 1 (count (set (map :db db-cursors))))
    :flame-out) ; TODO: Implement this
  (let [defaults {:no-sort false}
        conf     (merge defaults (apply hash-map conf-args))
        conf-obj (doto (JoinConfig.)
                   (.setNoSort (conf :no-sort)))
        db       (((first db-cursors) :db) :db)]
    (struct-map db-join-cursor
      :db      db
      :conf    conf
      :cursors db-cursors            ; TODO: Do we want the overhead of tracking these here?
      :join-cursor-handle (.join
                           (db :db-handle)
                           (into-array (map :cursor-handle db-cursors))
                           conf-obj))))


;; TODO: Error handling?
(defn db-join-cursor-close [db-join-cursor]
  (.close (db-join-cursor :join-cursor-handle)))


;; TODO: Error handling?
(defn db-join-cursor-next [db-join-cursor & opts-args]
  (let [defaults   {:lock-mode LockMode/DEFAULT}
        opts       (merge defaults (apply hash-map opts-args))
        key-entry  (marshal-db-entry* opts :key)
        data-entry (marshal-db-entry* opts :data)
        result     (.getNext
                    (db-join-cursor :join-cursor-handle)
                    key-entry data-entry (opts :lock-mode))]
    (unmarshal-db-entry* result key-entry data-entry)))
