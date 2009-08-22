(ns cupboard
  (:use [clojure.contrib java-utils])
  (:use [cupboard utils db-core])
  (:import [com.sleepycat.je DatabaseException]))



;;; ----------------------------------------------------------------------
;;; default variables for implicit use with Cupboard's public API
;;;
;;; These variables have meaningful global identity. Code which does
;;; not need to explicitly track its own Cupboard instances and which
;;; uses the common case of cupboard/with-transaction relies on them.
;;;
;;; Then, (1) macros which want to use them perform intentional
;;; variable capture on cupboard/*cupboard* (using the form
;;; ~'cupboard/*cupboard*) and use them in binding macros, and (2)
;;; functions which want to use them default to cupboard/*cupboard*
;;; and cupboard/*txn* as :cb and :txn optional arguments.
;;; ----------------------------------------------------------------------

(def *cupboard* nil)
(def *txn*      nil)



;;; ----------------------------------------------------------------------
;;; useful "constants"
;;; ----------------------------------------------------------------------

(def *shelves-db-name*    "_shelves")
(def *default-shelf-name* "_default")



;;; ----------------------------------------------------------------------
;;; useful structs
;;; ----------------------------------------------------------------------

(defstruct* cupboard
  :cb-env
  :shelves-db
  :shelves)


(defstruct* shelf
  :db
  :name
  :index-dbs)


(defstruct* persistence-metadata
  :shelf
  :primary-key
  :index-uniques
  :index-anys)



;;; ----------------------------------------------------------------------
;;; keep Clojure's compile-time symbol resolution happy
;;; ----------------------------------------------------------------------

(declare save)



;;; ----------------------------------------------------------------------
;;; cupboard maintenance
;;; ----------------------------------------------------------------------

(defn- get-index [cb shelf index-slot & opts-args]
  (let [defaults {:key-creator-fn    index-slot
                  :allow-create      true
                  :allow-populate    true
                  :transactional     true}
        opts     (merge defaults (args-map opts-args))]
    (if (contains? @(shelf :index-dbs) index-slot)
        ;; verify and return
        (let [index-db (@(shelf :index-dbs) index-slot)]
          ;; TODO: Handle this error better than with just an assert.
          (when (contains? opts :sorted-duplicates)
            (assert (= (-> index-db :conf :sorted-duplicates) (opts :sorted-duplicates))))
          index-db)
        ;; not open yet: open and return
        (let [[_ shelf-info] (db-get (cb :shelves-db) (shelf :name))
              indices        (shelf-info :indices)
              index-exists   (contains? indices index-slot)
              index-opts     (if index-exists
                                 (merge (indices index-slot) opts)
                                 (do (assert (contains? opts :sorted-duplicates))
                                     opts))
              index-db       (db-sec-open (cb :cb-env) (shelf :db) (str index-slot) index-opts)]
          ;; record index in shelf data structure
          (swap! (shelf :index-dbs) assoc index-slot index-db)
          ;; record index in (cb :shelves-db) if necessary
          (when-not index-exists
            (db-put (cb :shelves-db) (shelf :name)
                    (assoc (-> shelf :db :conf) :indices {index-slot index-opts})))
          index-db))))


(defn- close-shelf
  "The two-parameter form of close-shelf closes shelves and removes
  them from the cupboard object (cb argument). It does not remove the
  shelf from the cupboard on disk. The one-parameter form just closes
  the given shelf, without doing any extra cleanup."
  ([shelf]
     (doseq [index-db (vals @(shelf :index-dbs))]
       (db-sec-close index-db))
     (db-close (shelf :db)))
  ([cb shelf-name]
     (close-shelf (@(cb :shelves) shelf-name))
     (swap! (cb :shelves) dissoc shelf-name)))


(defn- close-shelves
  "If arg is a cupboard instance, this closes all cupboard shelves and
  removes them from the :shelves table in the cupboard. If arg is just
  a table of shelves, it simply closes them."
  [arg]
  (if (is-cupboard? arg)
      (doseq [shelf-name (keys @(arg :shelves))]
        (close-shelf arg shelf-name))
      ;; this is for use from the init-cupboard error recovery block
      (doseq [shelf (vals arg)]
        (close-shelf shelf))))


(defn- get-shelf
  "Returns the shelf identified by shelf-name from the cupboard
  identified by cb. If the shelf is not open, open and return it. If
  the shelf does not exist, then create, open, and return it."
  [cb shelf-name & opts-args]
  (let [defaults {:force-reopen      false
                  :deferred-write    false
                  :sorted-duplicates false
                  :read-only         false
                  :transactional     true}
        opts     (merge defaults (args-map opts-args))]
    (when (opts :force-reopen)
      (close-shelf cb shelf-name))
    (if (contains? @(cb :shelves) shelf-name)
        ;; shelf is ready and open, just return it
        (@(cb :shelves) shelf-name)
        ;; no shelf found in cupboard, need to either open or create it
        (let [shelf-desc (db-get (cb :shelves-db) shelf-name)]
          (if (= shelf-desc [])
              ;; shelf does not exist --- create a new one
              (let [new-shelf-opts {:deferred-write    (opts :deferred-write)
                                    :sorted-duplicates (opts :sorted-duplicates)
                                    :read-only         (opts :read-only)
                                    :transactional     (opts :transactional)}
                    new-shelf-db   (db-open (cb :cb-env) shelf-name
                                            (assoc new-shelf-opts :allow-create true))
                    new-shelf      (struct shelf new-shelf-db shelf-name (atom {}))]
                (db-put (cb :shelves-db) shelf-name (assoc new-shelf-opts :indices {}))
                (swap! (cb :shelves) assoc shelf-name new-shelf)
                new-shelf)
              ;; shelf exists --- open it
              (let [[_ shelf-opts] shelf-desc
                    ;; Careful on merge here! Only use explicitly specified arguments!
                    open-opts      (merge shelf-opts opts-args)
                    shelf-db       (db-open (cb :cb-env) shelf-name open-opts)
                    indices        (atom {})
                    shelf          (struct shelf shelf-db shelf-name indices)]
                ;; open shelf's indices
                (doseq [[index-slot index-opts] (shelf-opts :indices)]
                  (let [index-db (db-sec-open
                                  (cb :cb-env) (shelf :db) (str index-slot)
                                  :key-creator-fn    index-slot
                                  :allow-create      true
                                  :sorted-duplicates (index-opts :sorted-duplicates)
                                  :allow-populate    true
                                  :transactional     true)]
                    (swap! (shelf :index-dbs) assoc index-slot index-db)))
                ;; save the shelf in the cupboard
                (swap! (cb :shelves) assoc shelf-name shelf)
                ;; return the shelf
                shelf))))))


(defn- init-cupboard [cb-env cb-env-new]
  (let [shelves-db (db-open cb-env *shelves-db-name* :allow-create cb-env-new :transactional true)
        shelves    (atom {})
        cb         (struct cupboard cb-env shelves-db shelves)]
    (try
     (when cb-env-new
       (with-db default-shelf-db [cb-env *default-shelf-name*
                                  :allow-create true :transactional true]
         (db-put shelves-db *default-shelf-name*
                 {:deferred-write false :sorted-duplicates false
                  :read-only false :transactional true
                  :indices {}})))
     ;; load metadata about all shelves
     (with-db-cursor shelf-cursor [shelves-db]
       (loop [shelf-entry (db-cursor-first shelf-cursor)]
         (when-not (= shelf-entry [])
           (let [[shelf-name _] shelf-entry]
             (get-shelf cb shelf-name)
             (recur (db-cursor-next shelf-cursor))))))
     ;; return the cupboard
     cb
     ;; catch block must close all open databases
     (catch Exception e
       ;; TODO: LOGGING?
       (try
        (throw e)
        (finally
         (db-close shelves-db)
         (close-shelves @shelves)))))))


(defn open-cupboard [cb-dir-arg]
  (let [cb-dir     (file cb-dir-arg)
        cb-env-new (not (.exists cb-dir))]
    (when cb-env-new (.mkdir cb-dir))
    (let [cb-env (db-env-open cb-dir :allow-create cb-env-new :transactional true)]
      (try
       (init-cupboard cb-env cb-env-new)
       (catch Exception e
         ;; TODO: LOGGING?
         (try
          (throw e)
          (finally
           ;; TODO: Close everything else, too.
           (db-env-close cb-env))))))))


(defn close-cupboard [cb]
  (close-shelves cb)
  (db-close (cb :shelves-db))
  (db-env-close (cb :cb-env)))


;;; TODO: (defn list-shelves ...)
;;; TODO: (defn remove-shelf ...)



;;; ----------------------------------------------------------------------
;;; persistent structs
;;; ----------------------------------------------------------------------

(defn- filter-slots [slot-names slot-attrs target-key target-value]
  ;; Do not use maps because the result should must have the same
  ;; order as the input slot-names and slot-attrs parallel arrays.
  (let [csa (count slot-attrs)]
    (loop [res [] i 0]
      (if (= i csa)
          res
          (do (let [sa (nth slot-attrs i)]
                (if (and (contains? sa target-key) (= (sa target-key) target-value))
                    (recur (conj res (nth slot-names i)) (inc i))
                    (recur res (inc i)))))))))


(defmulti make-instance (fn [& args] (first args)))


(defmacro defpersist [name slots & opts-args]
  (let [slot-names  (map first slots)
        slot-attrs  (map (comp #(args-map %) rest) slots)
        slot-map    (zipmap slot-names slot-attrs)
        idx-uniques (filter-slots slot-names slot-attrs :index :unique)
        idx-anys    (filter-slots slot-names slot-attrs :index :any)
        defaults    {:shelf *default-shelf-name*}
        opts        (merge defaults (args-map opts-args))
        pkey        (if (contains? opts :primary-key)
                        (opts :primary-key)
                        (first idx-uniques))
        pmeta       (struct persistence-metadata
                            (opts :shelf)
                            pkey
                            (remove-vec #(= pkey %) idx-uniques)
                            idx-anys)]
    `(do (defstruct ~name ~@slot-names)
         (defmethod make-instance ~name [& instance-args#]
           ;; XXX: WHICH CUPBOARD TO SAVE TO???
           (let [inst# (with-meta (apply struct instance-args#) ~pmeta)]
             ;; TODO: Now save the object!
             inst#)))))



;;; ----------------------------------------------------------------------
;;; simple object saving and loading
;;; ----------------------------------------------------------------------

(defn save [obj & opts-args]
  (let [defaults {:cb  *cupboard*
                  :txn *txn*}
        opts     (merge defaults (args-map opts-args))
        cb       (opts :cb)
        txn      (opts :txn)
        pmeta    (meta obj)
        shelf    (get-shelf cb (pmeta :shelf))]
    ;; Verify that the shelf has open indices for pmeta :index-uniques
    ;; and :index-anys.
    (doseq [unique-index (pmeta :index-uniques)]
      (get-index cb shelf unique-index :sorted-duplicates false))
    (doseq [any-index (pmeta :index-anys)]
      (get-index cb shelf any-index :sorted-duplicates true))
    ;; Write object!
    (db-put (shelf :db) (obj (pmeta :primary-key)) obj)))
