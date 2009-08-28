(ns cupboard.db-core
  (:use [cupboard utils marshal])
  (:use [clojure.contrib java-utils])
  (:import [com.sleepycat.je DatabaseException DatabaseEntry LockMode CheckpointConfig]
           [com.sleepycat.je Environment EnvironmentConfig]
           [com.sleepycat.je Transaction TransactionConfig]
           [com.sleepycat.je Database DatabaseConfig]
           [com.sleepycat.je Cursor SecondaryCursor JoinCursor CursorConfig JoinConfig]
           [com.sleepycat.je SecondaryDatabase SecondaryConfig SecondaryKeyCreator]))



;;; ----------------------------------------------------------------------
;;; useful structs
;;; ----------------------------------------------------------------------

(defstruct* db-env
  :dir
  :transactional
  :env-handle)


(defstruct* txn
  :status
  :txn-handle)


(defstruct* db
  :name
  :sorted-duplicates
  :db-handle)


(defstruct* db-cursor
  :cursor-handle)


(defstruct* db-join-cursor
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
;;; convenience functions, macros, and maps
;;; ----------------------------------------------------------------------

(defmacro def-with-db-macro [macro-name open-fn close-fn]
  `(defmacro ~macro-name [[var# & open-args#] & body#]
     `(let [~var# (apply ~'~open-fn [~@open-args#])]
        (try
         ~@body#
         (finally (~'~close-fn ~var#))))))


(defonce *lock-modes*
  ;; NB: :serializable is not available here, as it does not make
  ;; sense outside a transaction.
  {:read-uncommitted LockMode/READ_UNCOMMITTED
   :dirty-read LockMode/READ_UNCOMMITTED
   :read-committed LockMode/READ_COMMITTED
   :default LockMode/DEFAULT
   :repeatable-read LockMode/DEFAULT
   :rmw LockMode/RMW
   :read-modify-write LockMode/RMW})



;;; ----------------------------------------------------------------------
;;; database environments
;;; ----------------------------------------------------------------------

(defn db-env-open [dir & conf-args]
  (let [defaults {:allow-create false
                  :read-only false
                  :transactional false
                  :shared-cache false
                  :txn-timeout 0        ; in microseconds
                  :txn-no-sync false
                  :txn-write-no-sync false
                  :txn-serializable-isolation false}
        dir (file dir)
        conf (merge defaults (args-map conf-args))
        conf-obj (doto (EnvironmentConfig.)
                   (.setAllowCreate (conf :allow-create))
                   (.setReadOnly (conf :read-only))
                   (.setTransactional (conf :transactional))
                   (.setSharedCache (conf :shared-cache))
                   (.setTxnTimeout (conf :txn-timeout))
                   (.setTxnNoSync (conf :txn-no-sync))
                   (.setTxnWriteNoSync (conf :txn-write-no-sync))
                   (.setTxnSerializableIsolation (conf :txn-serializable-isolation)))]
    (when-not (.exists dir) (.mkdir dir))
    (struct db-env
            dir
            (conf :transactional)
            (Environment. dir conf-obj))))


(defn db-env-close [db-env]
  (.cleanLog (db-env :env-handle))
  (.close (db-env :env-handle)))


(def-with-db-macro with-db-env db-env-open db-env-close)


(defn db-env-sync [db-env]
  (.sync (db-env :env-handle)))


(defn db-env-checkpoint [db-env & opts]
  (let [opts (args-map opts)
        cpc (when-not (empty? opts) (CheckpointConfig.))]
    (when (opts :force) (.setForce cpc true))
    (when (contains? opts :threshold-size) (.setKBytes (opts :threshold-size)))
    (when (contains? opts :threshold-time) (.setMinutes (opts :threshold-time)))
    (when (opts :minimize-recovery-time) (.setMinimizeRecoveryTime true))
    (.checkpoint (db-env :env-handle) cpc)))


(defn db-env-remove-db [db-env db-name & opts-args]
  (let [defaults {:txn nil}
        opts (merge defaults (args-map opts-args))]
    (.removeDatabase (db-env :env-handle) (opts :txn) db-name)))


(defn db-env-rename-db [db-env old-name new-name & opts-args]
  (let [defaults {:txn nil}
        opts (merge defaults (args-map opts-args))]
    (.renameDatabase (db-env :env-handle) (opts :txn) old-name new-name)))


(defn db-env-truncate-db [db-env db-name & opts-args]
  (let [defaults {:txn nil
                  :count false}
        opts (merge defaults (args-map opts-args))]
    (.truncateDatabase (db-env :env-handle) (opts :txn) db-name (opts :count))))


;; TODO: EnvironmentMutableConfig handling
;; TODO: Environment statistics gathering



;;; ----------------------------------------------------------------------
;;; transactions
;;; ----------------------------------------------------------------------

(defn db-txn-begin [db-env & conf-args]
  (let [defaults {:txn nil              ; parent transaction, if any
                  :no-sync false
                  :no-wait false
                  :isolation :repeatable-read}
        conf (merge defaults (args-map conf-args))
        conf-obj (let [co (TransactionConfig.)]
                   (.setNoSync co (conf :no-sync))
                   (.setNoWait co (conf :no-wait))
                   (cond
                     (= (conf :isolation) :read-uncommitted) (.setReadUncommitted co true)
                     (= (conf :isolation) :read-committed) (.setReadCommitted co true)
                     (= (conf :isolation) :serializable) (.setSerializableIsolation co true)))
        txn (struct txn
                    (atom :open)
                    (.beginTransaction (db-env :env-handle)
                                       (-> conf :txn :txn-handle)
                                       conf-obj))]
    txn))


(defn db-txn-commit [txn & conf-args]
  (let [defaults {:no-sync false
                  :write-no-sync false}
        conf (merge defaults (args-map conf-args))]
    (try
     (cond (conf :no-sync) (.commitNoSync (txn :txn-handle))
           (conf :write-no-sync) (.commitWriteNoSync (txn :txn-handle))
           :else (.commit (txn :txn-handle)))
     (reset! (txn :status) :committed)
     (catch DatabaseException de
       (reset! (txn :status) de)
       (throw de)))))


(defn db-txn-abort [txn]
  (try
   (.abort (txn :txn-handle))
   (reset! (txn :status) :aborted)
   (catch DatabaseException de
     (reset! (txn :status) de)
     (throw de))))


(def-with-db-macro with-db-txn db-txn-begin
  (fn [txn] (when (= @(txn :status) :open) (db-txn-commit txn))))



;;; ----------------------------------------------------------------------
;;; primary databases
;;; ----------------------------------------------------------------------

;; TODO: Add support for setting :btree-comparator and :duplicate-comparator
;; TODO: Add support for overriding :btree-comparator and :duplicate-comparator
;; (DatabaseConfig.setOverrideBtreeCompatator(), etc.)
(defn db-open [db-env name & conf-args]
  (let [defaults {:txn nil
                  :allow-create false
                  :deferred-write false
                  :temporary false
                  :sorted-duplicates false
                  :exclusive-create false
                  :read-only false
                  :transactional (db-env :transactional)}
        conf (merge defaults (args-map conf-args))
        conf-obj (doto (DatabaseConfig.)
                   (.setAllowCreate (conf :allow-create))
                   (.setDeferredWrite (conf :deferred-write))
                   (.setSortedDuplicates (conf :sorted-duplicates))
                   (.setExclusiveCreate (conf :exclusive-create))
                   (.setReadOnly (conf :read-only))
                   (.setTransactional (conf :transactional)))]
    (struct db
            name
            (conf :sorted-duplicates)
            (.openDatabase (db-env :env-handle)
                           (-> conf :txn :txn-handle)
                           name conf-obj))))


(defn db-close [db]
  (.close (db :db-handle)))


(def-with-db-macro with-db db-open db-close)


(defn db-sync [db]
  (let [db-handle (db :db-handle)]
    (when (.. db-handle getConfig getDeferredWrite)
      (.sync db-handle))))


;; TODO: (defn db-preload [db & preload-conf-args] ...)


(defn db-put
  "Optional keyword arguments:
     :no-dup-data  --- if true, then calls .putNoDupData
     :no-overwrite --- if true, then calls .putNoOverwrite"
  [db key data & opts-args]
  (let [defaults {:txn nil}
        opts (merge defaults (args-map opts-args))
        key-entry (marshal-db-entry key)
        data-entry (marshal-db-entry data)]
    (cond (opts :no-dup-data) (.putNoDupData (db :db-handle)
                                             (-> opts :txn :txn-handle)
                                             key-entry data-entry)
          (opts :no-overwrite) (.putNoOverwrite (db :db-handle)
                                                (-> opts :txn :txn-handle)
                                                key-entry data-entry)
          :else (.put (db :db-handle)
                      (-> opts :txn :txn-handle) key-entry data-entry))))


(defn db-get
  "Optional keyword arguments:
     :search-both --- uses Database.getSearchBoth with data specified in :data
     :data        --- if specified, can recycle DatabaseEntry; also used for getSearchBoth"
  [db key & opts-args]
  (let [defaults {:txn nil
                  :search-both false
                  :lock-mode :default}
        opts (merge defaults (args-map opts-args))
        key-entry (marshal-db-entry key)
        data-entry (marshal-db-entry* opts :data)
        lock-mode (*lock-modes* (opts :lock-mode))
        result (if (opts :search-both)
                   (.getSearchBoth (db :db-handle)
                                   (-> opts :txn :txn-handle)
                                   key-entry data-entry lock-mode)
                   (.get (db :db-handle)
                         (-> opts :txn :txn-handle)
                         key-entry data-entry lock-mode))]
    (unmarshal-db-entry* result key-entry data-entry)))


(defn db-delete [db key & opts-args]
  (let [defaults {:txn nil}
        opts (merge defaults (args-map opts-args))
        key-entry (marshal-db-entry key)]
    (.delete (db :db-handle) (-> opts :txn :txn-handle) key-entry)))


(defn db-count [db]
  (.count (db :db-handle)))



;;; ----------------------------------------------------------------------
;;; secondary databases (indices)
;;; ----------------------------------------------------------------------

(defn db-sec-open [db-env db-primary name & conf-args]
  (let [defaults {:txn nil
                  :key-creator-fn first
                  :allow-create false
                  :sorted-duplicates false
                  :allow-populate true
                  :transactional (db-env :transactional)}
        conf (merge defaults (args-map conf-args))
        key-creator (proxy [SecondaryKeyCreator] []
                      (createSecondaryKey [_ key-entry data-entry result-entry]
                        (let [data (unmarshal-db-entry data-entry)
                              sec-data ((conf :key-creator-fn) data)]
                          (if sec-data
                              (do (marshal-db-entry sec-data result-entry)
                                  true)
                              false))))
        conf-obj (doto (SecondaryConfig.)
                   (.setKeyCreator key-creator)
                   (.setAllowCreate (conf :allow-create))
                   (.setSortedDuplicates (conf :sorted-duplicates))
                   (.setAllowPopulate (conf :allow-populate))
                   (.setTransactional (conf :transactional)))]
    (struct db
            name
            (conf :sorted-duplicates)
            (.openSecondaryDatabase (db-env :env-handle)
                                    (-> conf :txn :txn-handle)
                                    name (db-primary :db-handle) conf-obj))))


(defn db-sec-close [db-sec]
  (.close (db-sec :db-handle)))


(def-with-db-macro with-db-sec db-sec-open db-sec-close)


(defn db-sec-get
  "Optional keyword arguments:
     :key  --- if specified, recycles DatabaseEntry
     :data --- if specified, recycles DatabaseEntry"
  [db-sec search-key & opts-args]
  (let [defaults {:txn nil
                  :lock-mode :default}
        opts (merge defaults (args-map opts-args))
        search-key-entry (marshal-db-entry search-key)
        key-entry (marshal-db-entry* opts :key)
        data-entry (marshal-db-entry* opts :data)
        result (.get (db-sec :db-handle)
                     (-> opts :txn :txn-handle)
                     search-key-entry key-entry data-entry
                     (*lock-modes* (opts :lock-mode)))]
    (unmarshal-db-entry* result key-entry data-entry)))


(defn db-sec-delete [db-sec search-key & opts-args]
  (let [defaults {:txn nil}
        opts (merge defaults (args-map opts-args))
        search-entry (marshal-db-entry search-key)]
    (.delete (db-sec :db-handle) (-> opts :txn :txn-handle) search-entry)))



;;; ----------------------------------------------------------------------
;;; database cursors
;;;
;;; This code supports both primary and secondary cursors, and treats them
;;; differently only where absolutely necessary. Calling code should simply
;;; pass the appropriate database into db-cursor-open.
;;; ----------------------------------------------------------------------

(defn db-cursor-open [db & conf-args]
  (let [defaults {:txn nil
                  :isolation :repeatable-read}
        conf (merge defaults (args-map conf-args))
        conf-obj (let [co (CursorConfig.)]
                   (cond
                     (= (conf :isolation) :read-uncommitted) (.setReadUncommitted co true)
                     (= (conf :isolation) :read-committed) (.setReadCommitted co true)))]
    (struct db-cursor
            (if (db-primary? db)
                (.openCursor (db :db-handle)
                             (-> conf :txn :txn-handle)
                             conf-obj)
                (.openSecondaryCursor (db :db-handle)
                                      (-> conf :txn :txn-handle)
                                      conf-obj)))))


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
  (let [defaults {:search-both false
                  :exact false
                  :lock-mode :default}
        opts (merge defaults (args-map opts-args))
        search-both (opts :search-both)
        exact (opts :exact)
        lock-mode (*lock-modes* (opts :lock-mode))
        key-entry (marshal-db-entry key)
        pkey-entry (when (db-cursor-sec? db-cursor) (marshal-db-entry* opts :pkey))
        data-entry (marshal-db-entry* opts :data)
        ;; search-fn1 is for primary database cursor lookups
        search-fn1 (cond
                     (and search-both exact) #(.getSearchBoth %1 %2 %3 %4)
                     search-both #(.getSearchBothRange %1 %2 %3 %4)
                     exact #(.getSearchKey %1 %2 %3 %4)
                     :else #(.getSearchKeyRange %1 %2 %3 %4))
        ;; search-fn2 is for secondary database cursor lookups
        search-fn2 (cond
                     (and search-both exact) #(.getSearchBoth %1 %2 %3 %4 %5)
                     search-both #(.getSearchBothRange %1 %2 %3 %4 %5)
                     exact #(.getSearchKey %1 %2 %3 %4 %5)
                     :else #(.getSearchKeyRange %1 %2 %3 %4 %5))
        result (if (db-cursor-primary? db-cursor)
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
     (let [defaults# {:lock-mode :default}
           opts# (merge defaults# (args-map opts-args#))
           key-entry# (marshal-db-entry* opts# :key)
           pkey-entry# (when (db-cursor-sec? db-cursor#) (marshal-db-entry* opts# :pkey))
           data-entry# (marshal-db-entry* opts# :data)
           lock-mode# (*lock-modes* (opts# :lock-mode))
           result# (if (db-cursor-primary? db-cursor#)
                       (~java-fn (db-cursor# :cursor-handle)
                                 key-entry# data-entry# lock-mode#)
                       (~java-fn (db-cursor# :cursor-handle)
                                 key-entry# pkey-entry# data-entry# lock-mode#))]
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
  (let [defaults {:direction :forward
                  :skip-dups false
                  :lock-mode :default}
        opts (merge defaults (args-map opts-args))
        direction (opts :direction)
        skip-dups (opts :skip-dups)
        key-entry (marshal-db-entry* opts :key)
        pkey-entry (when (db-cursor-sec? db-cursor) (marshal-db-entry* opts :pkey))
        data-entry (marshal-db-entry* opts :data)
        lock-mode (*lock-modes* (opts :lock-mode))
        ;; next-fn1 is for primary database cursors
        next-fn1 (cond
                   (and (= direction :forward) skip-dups) #(.getNextNoDup %1 %2 %3 %4)
                   (and (= direction :back) skip-dups) #(.getPrevNoDup %1 %2 %3 %4)
                   (= direction :forward) #(.getNext %1 %2 %3 %4)
                   (= direction :back) #(.getPrev %1 %2 %3 %4))
        ;; next-fn2 is for secondary database cursors
        next-fn2 (cond
                   (and (= direction :forward) skip-dups) #(.getNextNoDup %1 %2 %3 %4 %5)
                   (and (= direction :back) skip-dups) #(.getPrevNoDup %1 %2 %3 %4 %5)
                   (= direction :forward) #(.getNext %1 %2 %3 %4 %5)
                   (= direction :back) #(.getPrev %1 %2 %3 %4 %5))
        result (if (db-cursor-primary? db-cursor)
                   (next-fn1 (db-cursor :cursor-handle)
                             key-entry data-entry lock-mode)
                   (next-fn2 (db-cursor :cursor-handle)
                             key-entry pkey-entry data-entry lock-mode))]
    (unmarshal-db-entry* result
                         (if (db-cursor-primary? db-cursor) key-entry pkey-entry)
                         data-entry)))


(defn db-cursor-put [db-cursor key data & opts-args]
  (let [opts (args-map opts-args)
        key-entry (marshal-db-entry key)
        data-entry (marshal-db-entry data)]
    (cond (opts :no-dup-data) (.putNoDupData
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
        conf (merge defaults (args-map conf-args))
        conf-obj (doto (JoinConfig.)
                   (.setNoSort (conf :no-sort)))
        pdb-obj (.getPrimaryDatabase ((first db-cursors) :cursor-handle))]
    (struct db-join-cursor
            db-cursors
            (.join pdb-obj
                   (into-array (map :cursor-handle db-cursors))
                   conf-obj))))


(defn db-join-cursor-close [db-join-cursor]
  (.close (db-join-cursor :join-cursor-handle)))


(def-with-db-macro with-db-join-cursor db-join-cursor-open db-join-cursor-close)


(defn db-join-cursor-next [db-join-cursor & opts-args]
  (let [defaults {:lock-mode :default}
        opts (merge defaults (args-map opts-args))
        key-entry (marshal-db-entry* opts :key)
        data-entry (marshal-db-entry* opts :data)
        result (.getNext
                (db-join-cursor :join-cursor-handle)
                key-entry data-entry (*lock-modes* (opts :lock-mode)))]
    (unmarshal-db-entry* result key-entry data-entry)))
