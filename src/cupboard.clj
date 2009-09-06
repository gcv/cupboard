(ns cupboard
  (:use [clojure set])
  (:use [clojure.contrib def str-utils java-utils])
  (:use [cupboard utils db-core])
  (:import [com.sleepycat.je OperationStatus DatabaseException]))



;;; ----------------------------------------------------------------------------
;;; default variables for implicit use with Cupboard's public API
;;;
;;; These variables have meaningful global identity. Code which does not need to
;;; explicitly track its own Cupboard instances and which uses the common case
;;; of cupboard/with-transaction relies on them.
;;;
;;; Then, (1) macros which want to use them perform intentional variable capture
;;; on cupboard/*cupboard* (using the form ~'cupboard/*cupboard*) and use them
;;; in binding macros, and (2) functions which want to use them default to
;;; cupboard/*cupboard* and cupboard/*txn* as :cupboard and :txn optional
;;; arguments.
;;; ----------------------------------------------------------------------------

(defonce *cupboard* nil)
(defonce *txn* nil)



;;; ----------------------------------------------------------------------------
;;; useful "constants"
;;; ----------------------------------------------------------------------------

(defonce *shelves-db-name* "_shelves")
(defonce *default-shelf-name* "_default")



;;; ----------------------------------------------------------------------------
;;; useful structs
;;; ----------------------------------------------------------------------------

(defstruct* cupboard
  :cupboard-env
  :shelves-db
  :shelves)


(defstruct* shelf
  :db
  :name
  ;; Keep index types separate; this simplifies retrieval code since :unique and
  ;; :any indices require different treatment.
  :index-unique-dbs
  :index-any-dbs)


(defstruct* persistence-metadata
  ; :cupboard intentionally omitted
  ; :shelf-name intentionally omitted
  :primary-key
  :index-uniques
  :index-anys)



;;; ----------------------------------------------------------------------------
;;; keep Clojure's compile-time symbol resolution happy
;;; ----------------------------------------------------------------------------

(declare list-shelves)
(declare save)



;;; ----------------------------------------------------------------------------
;;; cupboard maintenance
;;; ----------------------------------------------------------------------------

(defn- close-shelf [cb shelf-name & opts-args]
  (let [defaults {:remove false
                  :txn *txn*}
        opts (merge defaults (args-map opts-args))
        txn (opts :txn)
        shelves (cb :shelves)
        shelf (@shelves shelf-name)]
    ;; close and dissociate index secondary databases
    (doseq [index-type [:index-unique-dbs :index-any-dbs]]
      (doseq [index-name (keys @(shelf index-type))]
        (let [index-db (@(shelf index-type) index-name)
              index-db-name (index-db :name)]
          ;; Closing the index database should be safe even without a
          ;; transaction; get-index ensures that an index database is open when
          ;; needed.
          (db-sec-close index-db)
          (swap! (shelf index-type) dissoc index-name)
          (when (opts :remove)
            (db-env-remove-db @(cb :cupboard-env) index-db-name :txn txn)
            (db-delete @(cb :shelves-db) index-db-name :txn txn)))))
    ;; close and dissociate the shelf primary database
    (db-close (shelf :db))
    (swap! shelves dissoc shelf-name)
    (when (opts :remove)
      (db-env-remove-db @(cb :cupboard-env) shelf-name :txn txn)
      (db-delete @(cb :shelves-db) shelf-name :txn txn))))


(defn- close-shelves [cb]
  (doseq [shelf-name (keys @(cb :shelves))]
    (close-shelf cb shelf-name)))


(defn- get-index [cb shelf index-name & opts-args]
  (let [opts (args-map opts-args)
        index-db-name (str (shelf :name) index-name)
        all-indices (merge @(shelf :index-unique-dbs) @(shelf :index-any-dbs))]
    (if (and (contains? all-indices index-name)
             (not (nil? @(-> all-indices index-name :db-handle))))
        ;; index already open, just return it
        (all-indices index-name)
        ;; else, need to open the index
        (let [[_ stored-index-opts] (db-get @(cb :shelves-db) index-db-name)
              index-opts (merge stored-index-opts
                                (select-keys opts [:sorted-duplicates]))
              index-open-opts (merge index-opts {:allow-create true
                                                 :key-creator-fn index-name})
              index-db (db-sec-open @(cb :cupboard-env) (shelf :db)
                                    index-db-name index-open-opts)]
          (db-put @(cb :shelves-db) index-db-name index-opts)
          (swap! (shelf (if (.. @(index-db :db-handle) getConfig getSortedDuplicates)
                            :index-any-dbs
                            :index-unique-dbs))
                 assoc index-name index-db)
          index-db))))


(defn- open-indices [cb shelf]
  (let [shelf-name (shelf :name)]
    (doseq [db-name (.getDatabaseNames @(@(cb :cupboard-env) :env-handle))]
      (let [[found-shelf-name index-name] (re-split #":" db-name)]
        (when (and (not (nil? index-name)) (= shelf-name found-shelf-name))
          (get-index cb shelf (keyword index-name)))))))


(defn- get-shelf
  "Returns the shelf identified by shelf-name from the cupboard identified by
   cb. If the shelf is not open, open and return it. If the shelf does not
   exist, then create, open, and return it."
  [cb shelf-name & opts-args]
  (let [defaults {:read-only false}
        opts (merge defaults (args-map opts-args))]
    (when (.contains shelf-name ":")
      (throw (RuntimeException. "shelf names cannot contain ':' characters")))
    (when (= shelf-name *shelves-db-name*)
      (throw (RuntimeException. (str "shelf name cannot be " *shelves-db-name*))))
    (when (opts :force-reopen)
      (close-shelf cb shelf-name))
    (if (contains? @(cb :shelves) shelf-name)
        ;; shelf is ready and open, just return it
        (@(cb :shelves) shelf-name)
        ;; else, need to open or create the shelf
        (let [[_ stored-shelf-opts] (db-get @(cb :shelves-db) shelf-name)
              shelf-opts (merge stored-shelf-opts (select-keys opts [])) ; fill this in as needed
              open-shelf-opts (merge shelf-opts
                                     (select-keys opts [:read-only])
                                     {:allow-create true
                                      :sorted-duplicates false})
              shelf-db (db-open @(cb :cupboard-env) shelf-name open-shelf-opts)
              shelf (struct shelf shelf-db shelf-name (atom {}) (atom {}))]
          (db-put @(cb :shelves-db) shelf-name shelf-opts)
          (swap! (cb :shelves) assoc shelf-name shelf)
          (open-indices cb shelf)
          shelf))))


(defn- init-cupboard [cb-env cb-env-new]
  (let [shelves-db (db-open cb-env *shelves-db-name*
                            :allow-create cb-env-new :transactional true
                            :sorted-duplicates false)
        cb (struct cupboard (atom cb-env) (atom shelves-db) (atom {}))]
    (try
     ;; open the default shelf explicitly in a new cupboard
     (when cb-env-new
       (get-shelf cb *default-shelf-name*))
     ;; open all shelves in the environment
     (doseq [db-name (list-shelves :cupboard cb)]
       (get-shelf cb db-name))
     ;; return the cupboard
     cb
     ;; catch block must close all open databases
     (catch Exception e
       (try
        (throw e)
        (finally
         (db-close shelves-db)
         (close-shelves cb)))))))


(defn open-cupboard [cb-dir-arg]
  (let [cb-dir (file cb-dir-arg)
        cb-env-new (do (when (and (.exists cb-dir) (.isFile cb-dir))
                         (throw (RuntimeException.
                                 (str cb-dir " is a file, not a database directory"))))
                       (when-not (or (.exists cb-dir) (.mkdir cb-dir))
                         (throw (RuntimeException. (str "failed to create " cb-dir))))
                       (empty? (seq (.list cb-dir))))]
    (let [cb-env (db-env-open cb-dir :allow-create cb-env-new :transactional true)]
      (try
       (init-cupboard cb-env cb-env-new)
       (catch Exception e
         (try
          (throw e)
          (finally
           (db-env-close cb-env))))))))


(defn close-cupboard [cb]
  (close-shelves cb)
  (db-close @(cb :shelves-db))
  (reset! (cb :shelves-db) nil)
  (db-env-close @(cb :cupboard-env))
  (reset! (cb :cupboard-env) nil))


(defmacro with-open-cupboard [[& args] & body]
  (let [[cb-var cb-dir] (if (= (count args) 2)
                            args
                            ['*cupboard* (first args)])]
    `(~(if (= cb-var '*cupboard*)
           'binding
           'let)
      [~cb-var (open-cupboard ~cb-dir)]
        (try
         ~@body
         (finally (close-cupboard ~cb-var))))))


(defn remove-shelf [shelf-name & opts-args]
  (let [defaults {:cupboard *cupboard*
                  :txn *txn*}
        opts (merge defaults (args-map opts-args))
        cb (opts :cupboard)
        txn (opts :txn)]
    (when-not (= (close-shelf cb shelf-name :remove true :txn txn)
                 OperationStatus/SUCCESS)
      (throw (RuntimeException. "failed to remove shelf " shelf-name)))))


(defn list-shelves [& opts-args]
  (let [defaults {:cupboard *cupboard*}
        opts (merge defaults (args-map opts-args))
        cb (opts :cupboard)]
    (filter #(and (not (.contains % ":")) (not (= % *shelves-db-name*)))
            (.getDatabaseNames @(@(cb :cupboard-env) :env-handle)))))



;;; ----------------------------------------------------------------------------
;;; persistent structs
;;; ----------------------------------------------------------------------------

(defn- filter-slots [slot-names slot-attrs target-key target-value]
  ;; Do not use maps because the result should must have the same order as the
  ;; input slot-names and slot-attrs parallel arrays.
  (let [csa (count slot-attrs)]
    (loop [res #{} i 0]
      (if (= i csa)
          res
          (do (let [sa (nth slot-attrs i)]
                (if (and (contains? sa target-key)
                         (= (sa target-key) target-value))
                    (recur (conj res (nth slot-names i)) (inc i))
                    (recur res (inc i)))))))))


(defmulti make-instance (fn [& args] (first args)))


(defmacro defpersist [name slots & opts-args]
  (let [slot-names (map first slots)
        slot-attrs (map (comp #(args-map %) rest) slots)
        slot-map (zipmap slot-names slot-attrs)
        idx-uniques (filter-slots slot-names slot-attrs :index :unique)
        idx-anys (filter-slots slot-names slot-attrs :index :any)
        opts (args-map opts-args)]
    `(do (defstruct ~name ~@slot-names)
         (defmethod make-instance ~name [& instance-args#]
           (let [[struct-args#
                  instance-kw-args#] (args-&rest-&keys instance-args#)
                 inst-kw-meta-args# (dissoc instance-kw-args# :txn :save)
                 save-instance# (or (not (contains? instance-kw-args# :save))
                                    (instance-kw-args# :save))
                 inst-kw-save-args# (select-keys instance-kw-args# [:txn])
                 inst-meta-base# (struct persistence-metadata
                                   (java.util.UUID/randomUUID)
                                   ~idx-uniques ~idx-anys)
                 inst-meta# (merge ~opts inst-kw-meta-args# inst-meta-base#)
                 inst# (with-meta (apply struct struct-args#) inst-meta#)]
             (when save-instance#
               (save inst# inst-kw-save-args#))
             inst#)))))



;;; ----------------------------------------------------------------------------
;;; cupboard transactions
;;; ----------------------------------------------------------------------------

(defmacro- check-txn [txn & body]
  `(let [txn# ~txn]                     ; avoid multiple evaluation
     (if (or (nil? txn#) (= @(txn# :status) :open))
         ~@body
         (throw (RuntimeException. "attempting to operate on a non-open transaction")))))


(defn begin-txn [& opts-args]
  (let [defaults {:cupboard *cupboard*
                  :parent-txn nil       ; XXX: Not supported in Berkeley DB JE yet.
                  :isolation :repeatable-read}
        opts (merge defaults (args-map opts-args))
        cb (opts :cupboard)]
    (db-txn-begin @(cb :cupboard-env)
                  :txn (opts :parent-txn)
                  :isolation (opts :isolation))))


(letfn [(parse-args [args]
          (cond (empty? args) [*txn* (args-map args)]
                (keyword? (first args)) [*txn* (args-map args)]
                :else [(first args) (args-map (rest args))]))]

  (defn commit [& args]
    (let [[txn opts] (parse-args args)]
      (check-txn txn
        (db-txn-commit txn opts))))

  (defn rollback [& args]
    (let [[txn opts] (parse-args args)]
      (check-txn txn
        (db-txn-abort txn)))))


(defmacro with-txn [[& args] & body]
  (let [[txn-var opts] (cond (empty? args) ['*txn* (args-map args)]
                             (keyword? (first args)) ['*txn* (args-map args)]
                             :else [(first args) (args-map (rest args))])]
    `(~(if (= txn-var '*txn*)
           'binding
           'let)
      [~txn-var (begin-txn ~opts)]
        (try
         ~@body
         (finally
          (when (= @(~txn-var :status) :open)
            (commit ~txn-var)))))))



;;; ----------------------------------------------------------------------------
;;; simple object saving, loading, and deleting
;;; ----------------------------------------------------------------------------

(defn save [obj & opts-args]
  (let [defaults {:cupboard *cupboard*
                  :shelf-name *default-shelf-name*
                  :txn *txn*}
        pmeta (meta obj)
        opts (merge defaults
                    (args-map opts-args)
                    (select-keys pmeta [:cupboard :shelf-name]))
        cb (opts :cupboard)
        txn (opts :txn)
        shelf (get-shelf cb (opts :shelf-name))
        pkey (pmeta :primary-key)]
    ;; Verify that the shelf has open indices for pmeta :index-uniques and
    ;; :index-anys.
    (doseq [unique-index (pmeta :index-uniques)]
      (get-index cb shelf unique-index :sorted-duplicates false))
    (doseq [any-index (pmeta :index-anys)]
      (get-index cb shelf any-index :sorted-duplicates true))
    ;; Write object!
    (let [res (check-txn txn
                (db-put (shelf :db) pkey obj :txn txn))]
      (if (= res OperationStatus/SUCCESS)
          (with-meta obj pmeta)
          ;; This exception should not occur, right? Shelves do not allow
          ;; duplicates, and none of the overwrite-affecting flags are used.
          (throw (RuntimeException. (str "failed to save " obj)))))))


(defn retrieve [index-slot indexed-value & opts-args]
  (let [defaults {:cupboard *cupboard*
                  :shelf-name *default-shelf-name*
                  :txn *txn*}
        opts (merge defaults (args-map opts-args))
        cb (opts :cupboard)
        txn (opts :txn)
        shelf (get-shelf cb (opts :shelf-name))
        index-unique-dbs @(shelf :index-unique-dbs)
        index-any-dbs @(shelf :index-any-dbs)]
    (letfn [(res->data [[pkey value]]
              (when-not (nil? value)
                (let [value-key-set (set (keys value))
                      metadata {:primary-key pkey
                                :index-uniques (intersection (set (keys index-unique-dbs))
                                                             value-key-set)
                                :index-anys (intersection (set (keys index-any-dbs))
                                                          value-key-set)}]
                  (with-meta value metadata))))]
      ;; If the index-slot is in :index-uniques, retrieve it and return as is.
      ;; If the index-slot is in :index-anys, retrieve a lazy sequence.
      (cond
        ;; uniques
        (contains? index-unique-dbs index-slot)
        (let [res (check-txn txn
                    (db-sec-get (index-unique-dbs index-slot) indexed-value
                                :txn txn))]
          (res->data res))
        ;; anys --- cannot use with-db-cursor on lazy sequences
        (contains? index-any-dbs index-slot)
        (let [idx-cursor (check-txn txn
                           (db-cursor-open (index-any-dbs index-slot) :txn txn))]
          (letfn [(idx-scan [cursor-fn & cursor-fn-args]
                    (try
                     (let [res (apply cursor-fn (cons idx-cursor cursor-fn-args))]
                       (if (or (empty? res)
                               (not (= ((second res) index-slot) indexed-value)))
                           (do (db-cursor-close idx-cursor)
                               (lazy-seq))
                           (lazy-seq (cons (res->data res) (idx-scan db-cursor-next)))))
                     (catch DatabaseException de
                       (db-cursor-close idx-cursor)
                       (throw de))))]
            (idx-scan db-cursor-search indexed-value)))
        ;; not retrieving an indexed slot
        :else (throw (RuntimeException. (str "attempting retrieve by slot "
                                             index-slot ", not indexed on shelf "
                                             (shelf :name))))))))


;; assoc! would be a better name, but Clojure's transient data structures have
;; taken over that role.
(defn assoc*
  "Just like clojure.core/assoc, but works on objects defined with defpersist
   and created with make-instance. Does not support the long form of assoc with
   multiple key-value pairs because it makes :txn specifications ambiguous."
  [obj key val & opts-args]
  (let [opts (args-map opts-args)]
    (save (assoc obj key val) opts)))


(defn dissoc*
  "Just like clojure.core/dissoc, but works on objects defined with defpersist
   and created with make-instance. Does not support the long form of dissoc with
   multiple key-value pairs because it makes :txn specifications ambiguous."
  [obj key & opts-args]
  (let [opts (args-map opts-args)]
    (save (dissoc obj key) opts)))


(defn conj*
  "See assoc*."
  [obj x & opts-args]
  (let [opts (args-map opts-args)]
    (save (conj obj x) opts)))


;;; XXX: disj* is not implemented because defpersist-defined objects are not
;;; sets.


;;; TODO: (defn delete ...)
