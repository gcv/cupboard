(ns cupboard.db-core
  (:use [cupboard utils marshal])
  (:use [clojure.contrib java-utils])
  (:import [com.sleepycat.je DatabaseException DatabaseEntry LockMode]
           [com.sleepycat.je EnvironmentConfig Environment]
           [com.sleepycat.je Database DatabaseConfig]
           [com.sleepycat.je Cursor SecondaryCursor JoinCursor CursorConfig JoinConfig]
           [com.sleepycat.je SecondaryDatabase SecondaryConfig SecondaryKeyCreator]))



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


(defstruct db-cursor
  :conf
  :cursor-handle)


(defstruct db-join-cursor
  :conf
  :cursors
  :join-cursor-handle)


(defn db-primary?
  "Returns true if the given struct represents a primary database."
  [s]
  (= Database (class (s :db-handle))))


(defn db-cursor-primary?
  "Returns true if the given struct represents a primary database cursor."
  [s]
  (= Cursor (class (s :cursor-handle))))


(defn db-cursor-sec?
  "Returns true if the given struct represents a secondary database cursor."
  [s]
  (= SecondaryCursor (class (s :cursor-handle))))



;;; ----------------------------------------------------------------------
;;; convenience macro definition macro
;;; ----------------------------------------------------------------------

(defmacro def-with-db-macro [macro-name open-fn close-fn]
  `(defmacro ~macro-name [var# [& open-args#] & body#]
     `(let [~var# (apply ~'~open-fn [~@open-args#])]
        (try
         ~@body#
         (finally (~'~close-fn ~var#))))))



;;; ----------------------------------------------------------------------
;;; database environments
;;; ----------------------------------------------------------------------

(defn db-env-open [dir & conf-args]
  (let [defaults {:allow-create  false
                  :read-only     false
                  :transactional false}
        dir      (file dir)
        conf     (merge defaults (args-map conf-args))
        conf-obj (doto (EnvironmentConfig.)
                   (.setAllowCreate   (conf :allow-create))
                   (.setReadOnly      (conf :read-only))
                   (.setTransactional (conf :transactional)))]
    (when-not (.exists dir) (.mkdir dir))
    (struct-map db-env
      :dir  dir
      :conf conf
      :env-handle (Environment. dir conf-obj))))


(defn db-env-close [db-env]
  (.cleanLog (db-env :env-handle))
  (.close (db-env :env-handle)))


(def-with-db-macro with-db-env db-env-open db-env-close)


(defn db-env-sync [db-env]
  (.sync (db-env :env-handle)))


;; TODO: EnvironmentMutableConfig handling
;; TODO: Environment statistics gathering



;;; ----------------------------------------------------------------------
;;; primary databases
;;; ----------------------------------------------------------------------

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
        conf     (merge defaults (args-map conf-args))
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


(defn db-close [db]
  (.close (db :db-handle)))


(def-with-db-macro with-db db-open db-close)


(defn db-sync [db]
  (when (.getDeferredWrite (db :conf))
    (.sync (db :db-handle))))


;; TODO: (defn db-preload [db & preload-conf-args] ...)
;; TODO: (defn db-remove [db-env name] ...)
;; TODO: (defn db-truncate [db-env name & truncate-conf-args] ...)
;; TODO: (defn db-verify ...)
;; args: {:txn handle :count false}


(defn db-put
  "Optional keyword arguments:
     :no-dup-data  --- if true, then calls .putNoDupData
     :no-overwrite --- if true, then calls .putNoOverwrite"
  [db key data & opts-args]
  (let [opts       (args-map opts-args)
        key-entry  (marshal-db-entry key)
        data-entry (marshal-db-entry data)]
    (cond (opts :no-dup-data)  (.putNoDupData
                                (db :db-handle) nil key-entry data-entry)
          (opts :no-overwrite) (.putNoOverwrite
                                (db :db-handle) nil key-entry data-entry)
          :else (.put (db :db-handle) nil key-entry data-entry))))


(defn db-get
  "Optional keyword arguments:
     :search-both --- uses Database.getSearchBoth with data specified in :data
     :data        --- if specified, can recycle DatabaseEntry; also used for getSearchBoth"
  [db key & opts-args]
  (let [defaults   {:search-both false
                    :lock-mode   LockMode/DEFAULT}
        opts       (merge defaults (args-map opts-args))
        key-entry  (marshal-db-entry key)
        data-entry (marshal-db-entry* opts :data)
        result     (if (opts :search-both)
                       (.getSearchBoth (db :db-handle) nil
                                       key-entry data-entry (opts :lock-mode))
                       (.get (db :db-handle) nil
                             key-entry data-entry (opts :lock-mode)))]
    (unmarshal-db-entry* result key-entry data-entry)))


(defn db-delete [db key]
  (let [key-entry (marshal-db-entry key)]
    (.delete (db :db-handle) nil key-entry)))



;;; ----------------------------------------------------------------------
;;; secondary databases (indices)
;;; ----------------------------------------------------------------------

(defn db-sec-open [db-env db-primary name & conf-args]
  (let [defaults    {:key-creator-fn    first
                     :allow-create      false
                     :sorted-duplicates false
                     :allow-populate    true
                     :transactional     false}
        conf        (merge defaults (args-map conf-args))
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
                      (.setAllowPopulate    (conf :allow-populate))
                      (.setTransactional    (conf :transactional)))]
    (struct-map db
      :name name
      :conf conf
      :db-handle (.openSecondaryDatabase
                  (db-env :env-handle)
                  nil
                  name (db-primary :db-handle) conf-obj))))


(defn db-sec-close [db-sec]
  (.close (db-sec :db-handle)))


(def-with-db-macro with-db-sec db-sec-open db-sec-close)


(defn db-sec-get
  "Optional keyword arguments:
     :key  --- if specified, recycles DatabaseEntry
     :data --- if specified, recycles DatabaseEntry"
  [db-sec search-key & opts-args]
  (let [defaults         {:lock-mode LockMode/DEFAULT}
        opts             (merge defaults (args-map opts-args))
        search-key-entry (marshal-db-entry search-key)
        key-entry        (marshal-db-entry* opts :key)
        data-entry       (marshal-db-entry* opts :data)
        result           (.get (db-sec :db-handle) nil
                               search-key-entry key-entry data-entry
                               (opts :lock-mode))]
    (unmarshal-db-entry* result key-entry data-entry)))


(defn db-sec-delete [db-sec search-key]
  (let [search-entry (marshal-db-entry search-key)]
    (.delete (db-sec :db-handle) nil search-entry)))



;;; ----------------------------------------------------------------------
;;; database cursors
;;;
;;; This code supports both primary and secondary cursors, and treats them
;;; differently only where absolutely necessary. Calling code should simply
;;; pass the appropriate database into db-cursor-open.
;;; ----------------------------------------------------------------------

(defn db-cursor-open [db & conf-args]
  (let [defaults {:read-committed   true
                  :read-uncommitted false}
        conf     (merge defaults (args-map conf-args))
        conf-obj (doto (CursorConfig.)
                   (.setReadCommitted   (conf :read-committed))
                   (.setReadUncommitted (conf :read-uncommitted)))]
    (struct-map db-cursor
      :conf conf
      :cursor-handle (if (db-primary? db)
                         (.openCursor (db :db-handle) nil conf-obj)
                         (.openSecondaryCursor (db :db-handle) nil conf-obj)))))


(defn db-cursor-close [db-cursor]
  (.close (db-cursor :cursor-handle)))


(def-with-db-macro with-db-cursor db-cursor-open db-cursor-close)


;; TODO: Write tests to check "both" search mode.
(defn db-cursor-search
  "Optional keyword arguments:
     :search-both --- use Database.getSearchBoth functions
     :pkey        --- for cursors on secondary databases only, specifies the primary key value
     :data        --- if specified, positions the cursor by both key and :data values
     :exact       --- if true, match the key and optional :data exactly"
  [db-cursor key & opts-args]
  (let [defaults    {:search-both false
                     :exact       false
                     :lock-mode   LockMode/DEFAULT}
        opts        (merge defaults (args-map opts-args))
        search-both (opts :search-both)
        exact       (opts :exact)
        lock-mode   (opts :lock-mode)
        key-entry   (marshal-db-entry key)
        pkey-entry  (when (db-cursor-sec? db-cursor) (marshal-db-entry* opts :pkey))
        data-entry  (marshal-db-entry* opts :data)
        ;; search-fn1 is for primary database cursor lookups
        search-fn1 (cond
                     (and search-both exact) #(.getSearchBoth %1 %2 %3 %4)
                     search-both             #(.getSearchBothRange %1 %2 %3 %4)
                     exact                   #(.getSearchKey %1 %2 %3 %4)
                     :else                   #(.getSearchKeyRange %1 %2 %3 %4))
        ;; search-fn2 is for secondary database cursor lookups
        search-fn2 (cond
                     (and search-both exact) #(.getSearchBoth %1 %2 %3 %4 %5)
                     search-both             #(.getSearchBothRange %1 %2 %3 %4 %5)
                     exact                   #(.getSearchKey %1 %2 %3 %4 %5)
                     :else                   #(.getSearchKeyRange %1 %2 %3 %4 %5))
        result     (if (db-cursor-primary? db-cursor)
                       (search-fn1 (db-cursor :cursor-handle)
                                   key-entry data-entry lock-mode)
                       (search-fn2 (db-cursor :cursor-handle)
                                   key-entry pkey-entry data-entry lock-mode))]
    (unmarshal-db-entry* result
                         (if (db-cursor-primary? db-cursor) key-entry pkey-entry)
                         data-entry)))


(defmacro def-db-cursor-simple-position [name java-fn]
  `(defn ~name
     "Optional keyword arguments:
        :pkey --- for cursors on secondary databases only, specifies the primary key value
        :key  --- if specified, reuses the given DatabaseEntry
        :data --- if specified, reuses the given DatabaseEntry"
     [db-cursor# & opts-args#]
     (let [defaults#   {:lock-mode LockMode/DEFAULT}
           opts#       (merge defaults# (args-map opts-args#))
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


(defn db-cursor-next
  "Optional keyword arguments:
     :key  --- if specified, reuses the given DatabaseEntry
     :data --- if specified, reuses the given DatabaseEntry"
  [db-cursor & opts-args]
  (let [defaults   {:direction :forward
                    :skip-dups false
                    :lock-mode LockMode/DEFAULT}
        opts       (merge defaults (args-map opts-args))
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


(defn db-cursor-put [db-cursor key data & opts-args]
  (let [opts       (args-map opts-args)
        key-entry  (marshal-db-entry key)
        data-entry (marshal-db-entry data)]
    (cond (opts :no-dup-data)  (.putNoDupData
                                (db-cursor :cursor-handle) key-entry data-entry)
          (opts :no-overwrite) (.putNoOverwrite
                                (db-cursor :cursor-handle) key-entry data-entry)
          :else (.put (db-cursor :cursor-handle) key-entry data-entry))))


(defn db-cursor-delete
  "Deletes the record the cursor currently points to."
  [db-cursor]
  (.delete (db-cursor :cursor-handle)))


(defn db-cursor-replace
  "Replaces the data entry of the record the cursor currently points to."
  [db-cursor new-data]
  (.putCurrent (db-cursor :cursor-handle) (marshal-db-entry new-data)))


;; TODO: Write functions to manipulate the cursor's cache mode


(defn db-join-cursor-open [db-cursors & conf-args]
  (let [defaults {:no-sort false}
        conf     (merge defaults (args-map conf-args))
        conf-obj (doto (JoinConfig.)
                   (.setNoSort (conf :no-sort)))
        pdb-obj  (.getPrimaryDatabase ((first db-cursors) :cursor-handle))]
    (struct-map db-join-cursor
      :conf    conf
      :cursors db-cursors
      :join-cursor-handle (.join
                           pdb-obj
                           (into-array (map :cursor-handle db-cursors))
                           conf-obj))))


(defn db-join-cursor-close [db-join-cursor]
  (.close (db-join-cursor :join-cursor-handle)))


(def-with-db-macro with-db-join-cursor db-join-cursor-open db-join-cursor-close)


(defn db-join-cursor-next [db-join-cursor & opts-args]
  (let [defaults   {:lock-mode LockMode/DEFAULT}
        opts       (merge defaults (args-map opts-args))
        key-entry  (marshal-db-entry* opts :key)
        data-entry (marshal-db-entry* opts :data)
        result     (.getNext
                    (db-join-cursor :join-cursor-handle)
                    key-entry data-entry (opts :lock-mode))]
    (unmarshal-db-entry* result key-entry data-entry)))
