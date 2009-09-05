(ns cupboard.db-core
  (:use [cupboard utils marshal])
  (:use [clojure.contrib java-utils])
  (:import [java.io File])
  (:import [com.sleepycat.je DatabaseException DatabaseEntry LockMode CacheMode]
           [com.sleepycat.je CheckpointConfig StatsConfig]
           [com.sleepycat.je Environment EnvironmentConfig EnvironmentMutableConfig]
           [com.sleepycat.je Transaction TransactionConfig]
           [com.sleepycat.je Database DatabaseConfig]
           [com.sleepycat.je Cursor SecondaryCursor JoinCursor CursorConfig JoinConfig]
           [com.sleepycat.je SecondaryDatabase SecondaryConfig SecondaryKeyCreator]))



;;; ----------------------------------------------------------------------------
;;; useful structs
;;; ----------------------------------------------------------------------------

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
  (= Database (class @(s :db-handle))))


(defn db-cursor-primary?
  "Returns true if the given struct represents a primary database cursor."
  [s]
  (= Cursor (class @(s :cursor-handle))))


(defn db-cursor-sec?
  "Returns true if the given struct represents a secondary database cursor."
  [s]
  (= SecondaryCursor (class @(s :cursor-handle))))



;;; ----------------------------------------------------------------------------
;;; convenience functions, macros, and maps
;;; ----------------------------------------------------------------------------

(defmacro def-with-db-macro [macro-name open-fn close-fn]
  `(defmacro ~macro-name [[var# & open-args#] & body#]
     `(let [~var# (apply ~'~open-fn [~@open-args#])]
        (try
         ~@body#
         (finally (~'~close-fn ~var#))))))


(defonce *lock-modes*
  ;; NB: :serializable is not available here, as it does not make sense outside
  ;; a transaction.
  {:read-uncommitted LockMode/READ_UNCOMMITTED
   :dirty-read LockMode/READ_UNCOMMITTED
   :read-committed LockMode/READ_COMMITTED
   :default LockMode/DEFAULT
   :repeatable-read LockMode/DEFAULT
   :rmw LockMode/RMW
   :read-modify-write LockMode/RMW})



;;; ----------------------------------------------------------------------------
;;; database environments
;;; ----------------------------------------------------------------------------

(defn db-env-open [dir & conf-args]
  (let [defaults {:allow-create false
                  :read-only false
                  :transactional false
                  :shared-cache false
                  :txn-timeout 0        ; in microseconds
                  :txn-no-sync false
                  :txn-write-no-sync false
                  :txn-serializable-isolation false}
        #^File dir (file dir)
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
    (when (contains? conf :cache-percent)
      (.setCachePercent conf-obj (conf :cache-percent)))
    (when (contains? conf :cache-bytes)
      (.setCacheSize conf-obj (conf :cache-bytes)))
    (when (contains? conf :db-log-max-bytes)
      (.setConfigParam conf-obj EnvironmentConfig/LOG_FILE_MAX (str (conf :db-log-max-bytes))))
    (when (contains? conf :in-memory-only)
      (.setConfigParam conf-obj EnvironmentConfig/LOG_MEM_ONLY (str (conf :in-memory-only))))
    (when-not (.exists dir) (.mkdir dir))
    (struct db-env
            dir
            (conf :transactional)
            (atom (Environment. dir conf-obj)))))


(defn db-env-close [db-env]
  (let [#^Environment env-handle @(db-env :env-handle)]
    (.cleanLog env-handle)
    (.close env-handle))
  (reset! (db-env :env-handle) nil))


(def-with-db-macro with-db-env db-env-open db-env-close)


(defn db-env-modify [db-env & opts-args]
  (let [opts (args-map opts-args)
        #^Environment env-handle @(db-env :env-handle)
        #^EnvironmentMutableConfig conf-obj (.getMutableConfig env-handle)]
    (when (contains? opts :cache-percent)
      (.setCachePercent conf-obj (opts :cache-percent)))
    (when (contains? opts :cache-bytes)
      (.setCacheSize conf-obj (opts :cache-bytes)))
    (when (contains? opts :txn-no-sync)
      (.setTxnNoSync conf-obj (opts :txn-no-sync)))
    (when (contains? opts :txn-write-no-sync)
      (.setTxnWriteNoSync conf-obj (opts :txn-write-no-sync)))
    (.setMutableConfig env-handle conf-obj)))


(defn db-env-remove-db [db-env db-name & opts-args]
  (let [defaults {:txn nil}
        opts (merge defaults (args-map opts-args))
        #^Environment env-handle @(db-env :env-handle)]
    (.removeDatabase env-handle (deref* (-> opts :txn :txn-handle)) db-name)))


(defn db-env-rename-db [db-env old-name new-name & opts-args]
  (let [defaults {:txn nil}
        opts (merge defaults (args-map opts-args))
        #^Environment env-handle @(db-env :env-handle)]
    (.renameDatabase env-handle (deref* (-> opts :txn :txn-handle))
                     old-name new-name)))


(defn db-env-truncate-db [db-env db-name & opts-args]
  (let [defaults {:txn nil
                  :count false}
        opts (merge defaults (args-map opts-args))
        #^Environment env-handle @(db-env :env-handle)]
    (.truncateDatabase env-handle (deref* (-> opts :txn :txn-handle))
                       db-name (opts :count))))


(defn db-env-sync [db-env]
  (.sync #^Environment @(db-env :env-handle)))


(defn db-env-checkpoint [db-env & opts]
  (let [opts (args-map opts)
        #^CheckpointConfig cpc (when-not (empty? opts) (CheckpointConfig.))
        #^Environment env-handle @(db-env :env-handle)]
    (when (opts :force) (.setForce cpc true))
    (when (contains? opts :threshold-kbytes) (.setKBytes cpc (opts :threshold-kbytes)))
    (when (contains? opts :threshold-mins) (.setMinutes cpc (opts :threshold-mins)))
    (when (opts :minimize-recovery-time) (.setMinimizeRecoveryTime cpc true))
    (.checkpoint env-handle cpc)))


(defn db-env-clean-log
  "Cleans database log files and prepares them for disposal at next checkpoint.
   Normally done by background thread. Returns number of log files cleaned. May
   be called repeatedly until it returns 0."
  [db-env]
  (.cleanLog #^Environment @(db-env :env-handle)))


(defn db-env-evict-memory
  "Keeps memory usage within defined cache boundaries. Normally done by
   background thread."
  [db-env]
  (.evictMemory #^Environment @(db-env :env-handle)))


(defn db-env-compress
  "Compresses in-memory data structures after deletes. Normally done by
   background thread."
  [db-env]
  (.compress #^Environment @(db-env :env-handle)))


(defn db-env-stats [db-env & conf-args]
  (let [defaults {:reset-stats false
                  ;; environment stats
                  :n-cache-bytes false
                  :n-cache-misses false
                  :n-fsyncs false
                  :n-random-reads false
                  :n-random-writes false
                  :n-seq-reads false
                  :n-seq-writes false
                  :n-total-log-bytes false
                  ;; transaction stats
                  :n-txn-active false
                  :n-txn-begins false
                  :n-txn-aborts false
                  :n-txn-commits false
                  ;; lock stats (slow mode!)
                  :n-lock-owners false
                  :n-read-locks false
                  :n-total-locks false
                  :n-lock-waiting-txns false
                  :n-write-locks false
                  ;; lock stats (fast mode)
                  :n-lock-requests false
                  :n-lock-waits false}
        conf (merge defaults (args-map conf-args))
        #^StatsConfig conf-obj (StatsConfig.)
        #^Environment env-handle @(db-env :env-handle)
        env-stats [:n-cache-bytes :n-cache-misses :n-fsyncs :n-random-reads
                   :n-random-writes :n-seq-reads :n-seq-writes :n-total-log-bytes]
        txn-stats [:n-txn-active :n-txn-begins :n-txn-aborts :n-txn-commits]
        lock-stats-slow [:n-lock-owners :n-read-locks :n-total-locks
                         :n-lock-waiting-txns :n-write-locks]
        lock-stats-fast [:n-lock-requests :n-lock-waits]
        result (atom {})]
    ;; enable fast statistics when possible
    (.setFast conf-obj (any? identity (vals (select-keys conf lock-stats-slow))))
    ;; reset statistics afterwards if requested
    (.setClear conf-obj (conf :reset-stats))
    ;; gather all requested statistics...
    (letfn [(add-result [k f stat-obj]
              (when (conf k) (swap! result assoc k (f stat-obj))))]
      ;; gather environment statistics if any requested
      (when (any? identity (vals (select-keys conf env-stats)))
        (let [#^EnvironmentStats env-stat-obj (.getStats env-handle conf-obj)]
          (add-result :n-cache-bytes #(.getCacheTotalBytes %) env-stat-obj)
          (add-result :n-cache-misses #(.getNCacheMiss %) env-stat-obj)
          (add-result :n-fsyncs #(.getNFSyncs %) env-stat-obj)
          (add-result :n-random-reads #(.getNRandomReads %) env-stat-obj)
          (add-result :n-random-writes #(.getNRandomWrites %) env-stat-obj)
          (add-result :n-seq-reads #(.getNSequentialReads %) env-stat-obj)
          (add-result :n-seq-writes #(.getNSequentialWrites %) env-stat-obj)
          (add-result :n-total-log-bytes #(.getTotalLogSize %) env-stat-obj)))
      ;; gather transaction statistics if any requested
      (when (any? identity (vals (select-keys conf txn-stats)))
        (let [#^TransactionStats txn-stat-obj (.getTransactionStats env-handle conf-obj)]
          (add-result :n-txn-active #(.getNActive %) txn-stat-obj)
          (add-result :n-txn-begins #(.getNBegins %) txn-stat-obj)
          (add-result :n-txn-aborts #(.getNAborts %) txn-stat-obj)
          (add-result :n-txn-commits #(.getNCommits %) txn-stat-obj)))
      ;; gather lock statistics if any requested
      (when (any? identity
                  (vals (select-keys
                         conf (concat lock-stats-slow lock-stats-fast))))
        (let [#^LockStats lock-stat-obj (.getLockStats env-handle conf-obj)]
          (add-result :n-lock-owners #(.getNOwners %) lock-stat-obj)
          (add-result :n-read-locks #(.getNReadLocks %) lock-stat-obj)
          (add-result :n-total-locks #(.getNTotalLocks %) lock-stat-obj)
          (add-result :n-lock-waiting-txns #(.getNWaiters %) lock-stat-obj)
          (add-result :n-write-locks #(.getNWriteLocks %) lock-stat-obj)
          (add-result :n-lock-requests #(.getNRequests %) lock-stat-obj)
          (add-result :n-lock-waits #(.getNWaits %) lock-stat-obj))))
    ;; ...and return them
    @result))



;;; ----------------------------------------------------------------------------
;;; transactions
;;; ----------------------------------------------------------------------------

(defn db-txn-begin [db-env & conf-args]
  (let [defaults {:txn nil              ; parent transaction, if any
                  :isolation :repeatable-read}
        conf (merge defaults (args-map conf-args))
        conf-obj (let [co (TransactionConfig.)]
                   (when (contains? conf :no-sync)
                     (.setNoSync co (conf :no-sync)))
                   (when (contains? conf :write-no-sync)
                     (.setWriteNoSync co (conf :write-no-sync)))
                   (when (contains? conf :no-wait)
                     (.setNoWait co (conf :no-wait)))
                   (cond
                     (= (conf :isolation) :read-uncommitted) (.setReadUncommitted co true)
                     (= (conf :isolation) :read-committed) (.setReadCommitted co true)
                     (= (conf :isolation) :serializable) (.setSerializableIsolation co true))
                   co)
        #^Environment env-handle @(db-env :env-handle)
        txn (struct txn
                    (atom :open)
                    (atom (.beginTransaction env-handle
                                             (deref* (-> conf :txn :txn-handle))
                                             conf-obj)))]
    txn))


(defn db-txn-commit [txn & conf-args]
  (let [defaults {:no-sync false
                  :write-no-sync false}
        conf (merge defaults (args-map conf-args))
        #^Transaction txn-handle @(txn :txn-handle)]
    (try
     (cond (conf :no-sync) (.commitNoSync txn-handle)
           (conf :write-no-sync) (.commitWriteNoSync txn-handle)
           :else (.commit txn-handle))
     (reset! (txn :status) :committed)
     (catch DatabaseException de
       (reset! (txn :status) de)
       (throw de))
     (finally (reset! (txn :txn-handle) nil)))))


(defn db-txn-abort [txn]
  (let [#^Transaction txn-handle @(txn :txn-handle)]
    (try
     (.abort txn-handle)
     (reset! (txn :status) :aborted)
     (catch DatabaseException de
       (reset! (txn :status) de)
       (throw de))
     (finally (reset! (txn :txn-handle) nil)))))


(def-with-db-macro with-db-txn db-txn-begin
  (fn [txn] (when (= @(txn :status) :open) (db-txn-commit txn))))



;;; ----------------------------------------------------------------------------
;;; primary databases
;;; ----------------------------------------------------------------------------

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
                   (.setTransactional (conf :transactional)))
        #^Environment env-handle @(db-env :env-handle)]
    (struct db
            name
            (conf :sorted-duplicates)
            (atom (.openDatabase env-handle
                                 (deref* (-> conf :txn :txn-handle))
                                 name conf-obj)))))


(defn db-close [db]
  (.close #^Database @(db :db-handle))
  (reset! (db :db-handle) nil))


(def-with-db-macro with-db db-open db-close)


(defn db-sync [db]
  (let [#^Database db-handle @(db :db-handle)]
    (when (.. db-handle getConfig getDeferredWrite)
      (.sync db-handle))))


(defn db-put
  "Optional keyword arguments:
     :no-dup-data  --- if true, then calls .putNoDupData
     :no-overwrite --- if true, then calls .putNoOverwrite"
  [db key data & opts-args]
  (let [defaults {:txn nil}
        opts (merge defaults (args-map opts-args))
        key-entry (marshal-db-entry key)
        data-entry (marshal-db-entry data)
        #^Database db-handle @(db :db-handle)]
    (cond (opts :no-dup-data) (.putNoDupData db-handle
                                             (deref* (-> opts :txn :txn-handle))
                                             key-entry data-entry)
          (opts :no-overwrite) (.putNoOverwrite db-handle
                                                (deref* (-> opts :txn :txn-handle))
                                                key-entry data-entry)
          :else (.put db-handle
                      (deref* (-> opts :txn :txn-handle)) key-entry data-entry))))


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
        #^Database db-handle @(db :db-handle)
        result (if (opts :search-both)
                   (.getSearchBoth db-handle
                                   (deref* (-> opts :txn :txn-handle))
                                   key-entry data-entry lock-mode)
                   (.get db-handle
                         (deref* (-> opts :txn :txn-handle))
                         key-entry data-entry lock-mode))]
    (unmarshal-db-entry* result key-entry data-entry)))


(defn db-delete [db key & opts-args]
  (let [defaults {:txn nil}
        opts (merge defaults (args-map opts-args))
        key-entry (marshal-db-entry key)
        #^Database db-handle @(db :db-handle)]
    (.delete db-handle (deref* (-> opts :txn :txn-handle)) key-entry)))


(defn db-count [db]
  (.count #^Database @(db :db-handle)))



;;; ----------------------------------------------------------------------------
;;; secondary databases (indices)
;;; ----------------------------------------------------------------------------

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
                   (.setTransactional (conf :transactional)))
        #^Environment env-handle @(db-env :env-handle)
        #^Database db-primary-handle @(db-primary :db-handle)]
    (struct db
            name
            (conf :sorted-duplicates)
            (atom (.openSecondaryDatabase env-handle
                                          (deref* (-> conf :txn :txn-handle))
                                          name db-primary-handle conf-obj)))))


(defn db-sec-close [db-sec]
  (.close #^SecondaryDatabase @(db-sec :db-handle))
  (reset! (db-sec :db-handle) nil))


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
        #^SecondaryDatabase db-sec-handle @(db-sec :db-handle)
        result (.get db-sec-handle
                     (deref* (-> opts :txn :txn-handle))
                     search-key-entry key-entry data-entry
                     (*lock-modes* (opts :lock-mode)))]
    (unmarshal-db-entry* result key-entry data-entry)))


(defn db-sec-delete [db-sec search-key & opts-args]
  (let [defaults {:txn nil}
        opts (merge defaults (args-map opts-args))
        search-entry (marshal-db-entry search-key)
        #^SecondaryDatabase db-sec-handle @(db-sec :db-handle)]
    (.delete db-sec-handle (deref* (-> opts :txn :txn-handle)) search-entry)))



;;; ----------------------------------------------------------------------------
;;; database cursors
;;;
;;; This code supports both primary and secondary cursors, and treats them
;;; differently only where absolutely necessary. Calling code should simply pass
;;; the appropriate database into db-cursor-open.
;;; ----------------------------------------------------------------------------

(defn db-cursor-open [db & conf-args]
  (let [defaults {:txn nil
                  :isolation :repeatable-read}
        conf (merge defaults (args-map conf-args))
        conf-obj (let [co (CursorConfig.)]
                   (cond
                     (= (conf :isolation) :read-uncommitted) (.setReadUncommitted co true)
                     (= (conf :isolation) :read-committed) (.setReadCommitted co true)))]
    (struct db-cursor
            (atom
             (if (db-primary? db)
                 (.openCursor #^Database @(db :db-handle)
                              (deref* (-> conf :txn :txn-handle))
                              conf-obj)
                 (.openSecondaryCursor #^SecondaryDatabase @(db :db-handle)
                                       (deref* (-> conf :txn :txn-handle))
                                       conf-obj))))))


(defn db-cursor-close [db-cursor]
  (.close #^Cursor @(db-cursor :cursor-handle))
  (reset! (db-cursor :cursor-handle) nil))


(def-with-db-macro with-db-cursor db-cursor-open db-cursor-close)


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
                     (and search-both exact) #(.getSearchBoth #^Cursor %1 %2 %3 %4)
                     search-both #(.getSearchBothRange #^Cursor %1 %2 %3 %4)
                     exact #(.getSearchKey #^Cursor %1 %2 %3 %4)
                     :else #(.getSearchKeyRange #^Cursor %1 %2 %3 %4))
        ;; search-fn2 is for secondary database cursor lookups
        search-fn2 (cond
                     (and search-both exact) #(.getSearchBoth #^SecondaryCursor %1 %2 %3 %4 %5)
                     search-both #(.getSearchBothRange #^SecondaryCursor %1 %2 %3 %4 %5)
                     exact #(.getSearchKey #^SecondaryCursor %1 %2 %3 %4 %5)
                     :else #(.getSearchKeyRange #^SecondaryCursor %1 %2 %3 %4 %5))
        result (if (db-cursor-primary? db-cursor)
                   (search-fn1 @(db-cursor :cursor-handle)
                               key-entry data-entry lock-mode)
                   (search-fn2 @(db-cursor :cursor-handle)
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
                       (~java-fn #^Cursor @(db-cursor# :cursor-handle)
                                 key-entry# data-entry# lock-mode#)
                       (~java-fn #^SecondaryCursor @(db-cursor# :cursor-handle)
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
                   (and (= direction :forward) skip-dups) #(.getNextNoDup #^Cursor %1 %2 %3 %4)
                   (and (= direction :back) skip-dups) #(.getPrevNoDup #^Cursor %1 %2 %3 %4)
                   (= direction :forward) #(.getNext #^Cursor %1 %2 %3 %4)
                   (= direction :back) #(.getPrev #^Cursor %1 %2 %3 %4))
        ;; next-fn2 is for secondary database cursors
        next-fn2 (cond
                   (and (= direction :forward) skip-dups) #(.getNextNoDup
                                                            #^SecondaryCursor %1 %2 %3 %4 %5)
                   (and (= direction :back) skip-dups) #(.getPrevNoDup
                                                         #^SecondaryCursor %1 %2 %3 %4 %5)
                   (= direction :forward) #(.getNext #^SecondaryCursor %1 %2 %3 %4 %5)
                   (= direction :back) #(.getPrev #^SecondaryCursor %1 %2 %3 %4 %5))
        result (if (db-cursor-primary? db-cursor)
                   (next-fn1 #^Cursor @(db-cursor :cursor-handle)
                             key-entry data-entry lock-mode)
                   (next-fn2 #^SecondaryCursor @(db-cursor :cursor-handle)
                             key-entry pkey-entry data-entry lock-mode))]
    (unmarshal-db-entry* result
                         (if (db-cursor-primary? db-cursor) key-entry pkey-entry)
                         data-entry)))


(defn db-cursor-put [db-cursor key data & opts-args]
  (let [opts (args-map opts-args)
        key-entry (marshal-db-entry key)
        data-entry (marshal-db-entry data)]
    (cond (opts :no-dup-data) (.putNoDupData
                               @(db-cursor :cursor-handle) key-entry data-entry)
          (opts :no-overwrite) (.putNoOverwrite
                                @(db-cursor :cursor-handle) key-entry data-entry)
          :else (.put @(db-cursor :cursor-handle) key-entry data-entry))))


(defn db-cursor-delete
  "Deletes the record the cursor currently points to."
  [db-cursor]
  (.delete @(db-cursor :cursor-handle)))


(defn db-cursor-replace
  "Replaces the data entry of the record the cursor currently points to."
  [db-cursor new-data]
  (.putCurrent @(db-cursor :cursor-handle) (marshal-db-entry new-data)))


(defn db-cursor-cache-mode [db-cursor mode]
  (let [mode-obj (cond (= mode :default) CacheMode/DEFAULT
                       (= mode :keep-hot) CacheMode/KEEP_HOT
                       (= mode :unchanged) CacheMode/UNCHANGED
                       :else (throw (RuntimeException. "invalid cursor cache mode")))]
    (.setCacheMode @(db-cursor :cursor-handle) mode-obj)))


(defn db-join-cursor-open [db-cursors & conf-args]
  (let [defaults {:no-sort false}
        conf (merge defaults (args-map conf-args))
        conf-obj (doto (JoinConfig.)
                   (.setNoSort (conf :no-sort)))
        #^Database pdb-obj (.getPrimaryDatabase @((first db-cursors) :cursor-handle))]
    (struct db-join-cursor
            db-cursors
            (atom (.join pdb-obj
                         (into-array (map (comp deref :cursor-handle) db-cursors))
                         conf-obj)))))


(defn db-join-cursor-close [db-join-cursor]
  (.close #^JoinCursor @(db-join-cursor :join-cursor-handle))
  (reset! (db-join-cursor :join-cursor-handle) nil))


(def-with-db-macro with-db-join-cursor db-join-cursor-open db-join-cursor-close)


(defn db-join-cursor-next [db-join-cursor & opts-args]
  (let [defaults {:lock-mode :default}
        opts (merge defaults (args-map opts-args))
        key-entry (marshal-db-entry* opts :key)
        data-entry (marshal-db-entry* opts :data)
        result (.getNext
                #^JoinCursor @(db-join-cursor :join-cursor-handle)
                key-entry data-entry (*lock-modes* (opts :lock-mode)))]
    (unmarshal-db-entry* result key-entry data-entry)))
