(ns cupboard
  (:use [clojure.contrib java-utils])
  (:use [cupboard utils db-core])
  (:import [com.sleepycat.je DatabaseException]))



;;; ----------------------------------------------------------------------
;;; useful "constants"
;;; ----------------------------------------------------------------------

(def *shelves-db-name*    "__shelves__")
(def *default-shelf-name* "__default__")



;;; ----------------------------------------------------------------------
;;; useful structs
;;; ----------------------------------------------------------------------

(defstruct cupboard
  :cb-env
  :shelves-db
  :shelves)


(defstruct shelf
  :db                                   ; XXX: Maybe this is the wrong place to track the db!
  :name
  :count
  :transactional
  :index-names)


(defstruct persistence-metadata
  :primary-key
  :indexed-slots)



;;; ----------------------------------------------------------------------
;;; cupboard maintenance
;;; ----------------------------------------------------------------------

(defn- init-cupboard [cb-env cb-env-new]
  (let [shelves-db (db-open cb-env *shelves-db-name*
                            :allow-create cb-env-new :transactional true)
        shelves    (atom {})]
    (try
     (when cb-env-new
       (with-db default-shelf-db [cb-env *default-shelf-name*
                                  :allow-create true :transactional true]
         (db-put shelves-db *default-shelf-name*
                 (struct-map shelf
                   :name *default-shelf-name*
                   :count 0
                   :transactional true
                   :index-names []))))
     ;; load metadata about all shelves
     (with-db-cursor shelf-cursor [shelves-db]
       (loop [shelf-entry (db-cursor-first shelf-cursor)]
         (when-not (= shelf-entry [])
           (let [[shelf-name shelf-value] shelf-entry
                 ;; open the shelf database
                 shelf-db (db-open cb-env shelf-name :transactional (shelf-value :transactional))
                 ;; TODO: open indices
                 ]
             (swap! shelves assoc shelf-name
                    (assoc shelf-value :db shelf-db :count (atom shelf-value))))
           (recur (db-cursor-next shelf-cursor)))))
     ;; return the cupboard
     (struct-map cupboard
       :cb-env cb-env
       :shelves-db shelves-db
       :shelves shelves)
     ;; catch block must close all open databases
     (catch Exception e
       ;; TODO: LOGGING?
       (try
        (throw e)
        (finally
         (db-close shelves-db)
         (map db-close (vals @shelves))))))))


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
  ;; close all shelves
  
  ;; close shelves-db
  ;; close cb-env
)
