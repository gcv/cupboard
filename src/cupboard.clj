(ns cupboard
  (:use [clojure.contrib java-utils])
  (:use [cupboard utils db-core])
  (:import [com.sleepycat.je DatabaseException]))



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
;;; cupboard maintenance
;;; ----------------------------------------------------------------------

(defn- get-shelf
  "Returns the shelf identified by shelf-name from the cupboard
  identified by cb. If the shelf is not open, open and return it. If
  the shelf does not exist, then create, open, and return it."
  [cb shelf-name & opts-args]
  (let [defaults {:deferred-write    false
                  :sorted-duplicates false
                  :read-only         false
                  :transactional     true
                  :force-reopen      false}
        opts     (merge defaults (args-map opts-args))]
    (when (opts :force-reopen)
      (close-shelf cb shelf-name))
    (if (contains? @(cb :shelves) shelf-name)
        ;; shelf is ready and open, just return it
        ((cb :shelves) shelf-name)
        ;; no shelf found in cupboard, need to either open or create it
        (let [shelf-desc (db-get (cb :shelves-db) shelf-name)]
          (if (= shelf-desc [])
              ;; shelf does not exist --- create a new one
              ;; TODO: Refactor into a separate function, and use in init-cupboard
              (let [new-shelf-opts {:deferred-write    (opts :deferred-write)
                                    :sorted-duplicates (opts :sorted-duplicates)
                                    :read-only         (opts :read-only)
                                    :transactional     (opts :transactional)}
                    new-shelf-db   (db-open (cb :cb-env) shelf-name
                                            (assoc new-shelf-opts :allow-create true))
                    new-shelf      (struct shelf new-shelf-db shelf-name (atom {}))]
                (db-put (cb :shelves-db) shelf-name (assoc new-shelf-opts :index-names []))
                (swap! (cb :shelves) assoc shelf-name new-shelf)
                new-shelf)
              ;; shelf exists --- open it
              ;; TODO: Refactor into a separate function, and use in init-cupboard
              (let [[_ shelf-opts] shelf-desc
                    ;; Careful on merge here! Only use explicitly specified arguments!
                    open-opts      (merge shelf-opts opts-args)
                    shelf-db       (db-open (cb :cb-env) shelf-name open-opts)
                    ;; TODO: Open indices!
                    shelf          (struct shelf shelf-db shelf-name (atom {}))]
                (swap! (cb :shelves) assoc shelf-name shelf)
                shelf))))))


;; TODO: Check that index closing actually works.
(defn- close-shelf
  "The two-parameter form of close-shelf closes shelves and removes
  them from the cupboard (cb argument). The one-parameter form just
  closes the given shelf, without doing any extra cleanup."
  ([shelf]
     (doseq [index-db (vals @(shelf :index-dbs))]
       (db-close (index-db :db)))
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
      (doseq [shelf-name (vals @(arg :shelves))]
        (close-shelf arg shelf-name))
      ;; this is for use from the init-cupboard error recovery block
      (doseq [shelf (vals arg)]
        (close-shelf shelf))))


(defn- init-cupboard [cb-env cb-env-new]
  (let [shelves-db (db-open cb-env *shelves-db-name* :allow-create cb-env-new :transactional true)
        shelves    (atom {})]
    (try
     (when cb-env-new
       (with-db default-shelf-db [cb-env *default-shelf-name*
                                  :allow-create true :transactional true]
         (db-put shelves-db *default-shelf-name*
                 {:deferred-write false :sorted-duplicates false
                  :read-only false :transactional true
                  :index-names []})))
     ;; load metadata about all shelves
     (with-db-cursor shelf-cursor [shelves-db]
       (loop [shelf-entry (db-cursor-first shelf-cursor)]
         (when-not (= shelf-entry [])
           (let [[shelf-name shelf-value] shelf-entry
                 ;; open the shelf's database
                 shelf-db (db-open cb-env shelf-name
                                   :deferred-write    (shelf-value :deferred-write)
                                   :sorted-duplicates (shelf-value :sorted-duplicates)
                                   :read-only         (shelf-value :read-only)
                                   :transactional     (shelf-value :transactional))
                 ;; TODO: open indices
                 ]
             (swap! shelves assoc shelf-name
                    (struct shelf shelf-db shelf-name (atom {}))))
           (recur (db-cursor-next shelf-cursor)))))
     ;; return the cupboard
     (struct-map cupboard
       :cb-env     cb-env
       :shelves-db shelves-db
       :shelves    shelves)
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
           (db-env-close cb-env))))))))


(defn close-cupboard [cb]
  (close-shelves @(cb :shelves))
  (db-close (cb :shelves-db))
  (db-env-close (cb :cb-env)))



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
        pmeta       (struct persistence-metadata (opts :shelf) pkey idx-uniques idx-anys)]
    `(do (defstruct ~name ~@slot-names)
         (defmethod make-instance ~name [& instance-args#]
           (with-meta (apply struct instance-args#) ~pmeta)))))
