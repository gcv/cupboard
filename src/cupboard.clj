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

(defstruct cupboard
  :cb-env
  :shelves-db
  :shelves)


(defstruct shelf
  :db
  :name
  :index-dbs)


(defstruct persistence-metadata
  :primary-key
  :indexed-slots)



;;; ----------------------------------------------------------------------
;;; cupboard maintenance
;;; ----------------------------------------------------------------------

;; TODO: Check that index closing actually works.
(defn- close-shelves [shelves]
  (doseq [shelf (vals shelves)]
    (doseq [index-db (vals @(shelf :index-dbs))]
      (db-close (index-db :db)))
    (db-close (shelf :db))))


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

(defn- filter-index-slots [slot-map index-type]
  (keys (filter (fn [[slot-name slot-attr]]
                  (and (contains? slot-attr :index)
                       (= (slot-attr :index) index-type)))
                slot-map)))


(defmulti make-instance first)


(defmacro defpersist [name slots & opts-args]
  (let [slot-names  (map first slots)
        slot-attrs  (map (comp #(apply hash-map %) rest) slots)
        slot-map    (zipmap slot-names slot-attrs)
        idx-uniques (filter-index-slots slot-map :unique)
        idx-anys    (filter-index-slots slot-map :any)
        defaults    {:shelf *default-shelf-name*}
        opts        (merge defaults (apply hash-map opts-args))]
    {:slot-names slot-names
     :uniques    idx-uniques
     :anys       idx-anys
     :options    opts}
  ;; expands into:
  ;;  - defstruct
  ;;  - defmethod make-instance specialized on name
  ))


(defpersist president
  ((:login      :index :unique)
   (:first-name :index :any)
   (:last-name  :index :any)
   (:age        :index :any)
   (:bank-acct  :index :unique))
  :primary-key :login
  :shelf "presidents")
