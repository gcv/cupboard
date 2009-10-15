(ns cupboard.core
  (:use clojure.set)
  (:use [clojure.contrib def str-utils java-utils])
  (:use cupboard.utils cupboard.bdb.je)
  (:import [com.sleepycat.je Environment OperationStatus DatabaseException DeadlockException]))



;;; ----------------------------------------------------------------------------
;;; default variables for implicit use with Cupboard's public API
;;;
;;; These variables have meaningful global identity. Code which does not need to
;;; explicitly track its own Cupboard instances and which uses the common case
;;; of cupboard.core/with-transaction relies on them.
;;;
;;; Then, (1) macros which want to use them perform intentional variable capture
;;; on cupboard.core/*cupboard* (using the form ~'cupboard.core/*cupboard*) and
;;; use them in binding macros, and (2) functions which want to use them default
;;; to cupboard.core/*cupboard* and cupboard.core/*txn* as :cupboard and :txn
;;; optional arguments.
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

(defstruct cupboard
  :cupboard-env
  :shelves-db
  :shelves)


(defstruct shelf
  :db
  :name
  ;; Keep index types separate; this simplifies retrieval code since :unique and
  ;; :any indices require different treatment.
  :index-unique-dbs
  :index-any-dbs)


(defstruct persistence-metadata
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
                  :truncate false
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
          (try
           (db-sec-close index-db)
           (finally (swap! (shelf index-type) dissoc index-name)))
          (when (opts :remove)
            (db-env-remove-db @(cb :cupboard-env) index-db-name :txn txn)
            (db-delete @(cb :shelves-db) index-db-name :txn txn))
          (when (opts :truncate)
            (db-env-truncate-db @(cb :cupboard-env) index-db-name :txn txn)))))
    ;; close and dissociate the shelf primary database
    (try
     (db-close (shelf :db))
     (finally (swap! shelves dissoc shelf-name)))
    (when (opts :remove)
      (db-env-remove-db @(cb :cupboard-env) shelf-name :txn txn)
      (db-delete @(cb :shelves-db) shelf-name :txn txn))
    (when (opts :truncate)
      (db-env-truncate-db @(cb :cupboard-env) shelf-name :txn txn))))


(defn- close-shelves [cb]
  (doseq [shelf-name (keys @(cb :shelves))]
    (close-shelf cb shelf-name)))


(defn- get-index [cb shelf index-name & opts-args]
  (let [opts (args-map opts-args)
        index-db-name (str (shelf :name) index-name)
        index-lookup (fn []
                       (let [all-indices (merge @(shelf :index-unique-dbs)
                                                @(shelf :index-any-dbs))]
                         (when (and (contains? all-indices index-name)
                                    (not (nil? @(-> all-indices index-name :db-sec-handle))))
                           (all-indices index-name))))]
    (if-let [index (index-lookup)]
      index
      (locking (keyword index-db-name) ; XXX: (keyword ...) ensures use of same object as mutex
        (if-let [index (index-lookup)]
          index
          ;; else, need to open the index
          (let [[_ stored-index-opts] (db-get @(cb :shelves-db) index-db-name)
                index-opts (merge stored-index-opts
                                  (select-keys opts [:sorted-duplicates]))
                index-open-opts (merge index-opts {:allow-create true
                                                   :key-creator-fn index-name})
                index-db (db-sec-open @(cb :cupboard-env) (shelf :db)
                                      index-db-name index-open-opts)]
            ;; only save index metadata when necessary
            (when-not (= stored-index-opts index-opts)
              (when-not (= (db-put @(cb :shelves-db) index-db-name index-opts)
                           OperationStatus/SUCCESS)
                (throw (RuntimeException. (str "failed to save metadata about index "
                                               index-db-name)))))
            (swap! (shelf (if (.. @(index-db :db-sec-handle) getConfig getSortedDuplicates)
                              :index-any-dbs
                              :index-unique-dbs))
                   assoc index-name index-db)
            index-db))))))


(defn- open-indices [cb shelf]
  (let [shelf-name (shelf :name)]
    (doseq [db-name (.getDatabaseNames #^Environment @(@(cb :cupboard-env) :env-handle))]
      (let [[found-shelf-name index-name] (re-split #":" db-name)]
        (when (and (not (nil? index-name)) (= shelf-name found-shelf-name))
          (get-index cb shelf (keyword index-name)))))))


(defn- get-shelf
  "Returns the shelf identified by shelf-name from the cupboard identified by
   cb. If the shelf is not open, open and return it. If the shelf does not
   exist, then create, open, and return it."
  [cb
   #^String shelf-name
   & opts-args]
  (let [opts (args-map opts-args)]
    (when (.contains shelf-name ":")
      (throw (RuntimeException. "shelf names cannot contain ':' characters")))
    (when (= shelf-name *shelves-db-name*)
      (throw (RuntimeException. (str "shelf name cannot be " *shelves-db-name*))))
    (when (opts :force-reopen)
      (close-shelf cb shelf-name))
    (if (contains? @(cb :shelves) shelf-name)
      ;; shelf is ready and open, just return it
      (@(cb :shelves) shelf-name)
      ;; not open; must carefully open it
      (locking (keyword shelf-name)
        ;; check again in case this thread blocked on the lock
        (if (contains? @(cb :shelves) shelf-name)
            (@(cb :shelves) shelf-name)
            ;; else, need to open or create the shelf
            (let [[_ stored-shelf-opts] (db-get @(cb :shelves-db) shelf-name)
                  shelf-opts (merge stored-shelf-opts (select-keys opts [])) ; fill in as needed
                  open-shelf-opts (merge shelf-opts
                                         {:allow-create true
                                          :sorted-duplicates false})
                  shelf-db (db-open @(cb :cupboard-env) shelf-name open-shelf-opts)
                  shelf (struct shelf shelf-db shelf-name (atom {}) (atom {}))]
              ;; save shelf metadata when necessary
              (when-not (= stored-shelf-opts shelf-opts)
                (when-not (= (db-put @(cb :shelves-db) shelf-name shelf-opts)
                             OperationStatus/SUCCESS)
                  (throw (RuntimeException. (str "failed to save metadata about shelf "
                                                 shelf-name)))))
              (swap! (cb :shelves) assoc shelf-name shelf)
              (open-indices cb shelf)
              shelf))))))


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


(defn open-cupboard [cb-dir-arg & opts-args]
  (let [#^java.io.File cb-dir (file cb-dir-arg)
        cb-env-new (do (when (and (.exists cb-dir) (.isFile cb-dir))
                         (throw (RuntimeException.
                                 (str cb-dir " is a file, not a database directory"))))
                       (when-not (or (.exists cb-dir) (.mkdir cb-dir))
                         (throw (RuntimeException. (str "failed to create " cb-dir))))
                       (empty? (seq (.list cb-dir))))]
    (let [cb-env (db-env-open cb-dir (merge (args-map opts-args)
                                            {:allow-create cb-env-new :transactional true}))]
      (try
       (init-cupboard cb-env cb-env-new)
       (catch Exception e
         (try
          (throw e)
          (finally
           (try
            (db-env-close cb-env)
            (catch Exception e2
              (throw (RuntimeException. (str "nested exceptions:\n"
                                             (.getMessage e2) "\n"
                                             (.getMessage e)))))))))))))


(defn open-cupboard!
  "Like open-cupboard, but always opens the global *cupboard*."
  [& args]
  (when (nil? *cupboard*)
    (def *cupboard* (apply open-cupboard args))))


(defn close-cupboard [cb]
  (close-shelves cb)
  (try
   (db-close @(cb :shelves-db))
   (finally (reset! (cb :shelves-db) nil)))
  (try
   (db-env-close @(cb :cupboard-env))
   (finally (reset! (cb :cupboard-env) nil))))


(defn close-cupboard!
  "Like close-cupboard, but always closes the global *cupboard*."
  []
  (when-not (nil? *cupboard*)
    (try
     (close-cupboard *cupboard*)
     (finally (def *cupboard* nil)))))


(defmacro with-open-cupboard [[& args] & body]
  (let [[cb-var cb-dir opts]
        (condp = (count args)
          0 (throw (RuntimeException. "invalid with-open-cupboard call"))
          1 ['cupboard.core/*cupboard* (first args) {}]
          2 [(first args) (second args) {}]
          ;; else
          (if (symbol? (first args))
              [(first args) (second args) (nnext args)]
              ['cupboard.core/*cupboard* (first args) (next args)]))]
    `(~(if (= cb-var 'cupboard.core/*cupboard*)
           'binding
           'let)
      [~cb-var (apply open-cupboard [~cb-dir ~@opts])]
        (try
         ~@body
         (finally (close-cupboard ~cb-var))))))


(defn remove-shelf [shelf-name & opts-args]
  (let [defaults {:cupboard *cupboard*
                  :txn *txn*}
        opts (merge defaults (args-map opts-args))
        cb (opts :cupboard)
        txn (opts :txn)]
    (close-shelf cb shelf-name :remove true :txn txn)))


(defn list-shelves [& opts-args]
  (let [defaults {:cupboard *cupboard*}
        opts (merge defaults (args-map opts-args))
        cb (opts :cupboard)]
    (filter #(and (not (.contains #^String % ":")) (not= % *shelves-db-name*))
            (.getDatabaseNames #^Environment @(@(cb :cupboard-env) :env-handle)))))


(defn shelf-count [& opts-args]
  (let [defaults {:cupboard *cupboard*
                  :shelf-name *default-shelf-name*}
        opts (merge defaults (args-map opts-args))
        cb (opts :cupboard)
        shelf-name (opts :shelf-name)
        shelf (get-shelf cb shelf-name)]
    (db-count (shelf :db))))


(defn clear-shelf
  "Deletes all contents of the shelf, but does not delete the shelf itself or
   any of its indices."
  [& opts-args]
  (let [defaults {:cupboard *cupboard*
                  :shelf-name *default-shelf-name*}
        opts (merge defaults (args-map opts-args))
        cb (opts :cupboard)
        shelf-name (opts :shelf-name)
        pass-through-args (merge
                           {:truncate true}
                           (dissoc opts :remove :cupboard :shelf-name))]
    (close-shelf cb shelf-name pass-through-args)
    ;; reopen the shelf
    (get-shelf cb shelf-name (dissoc pass-through-args :truncate))
    nil))


(defn verify-cupboard-env [& opts-args]
  (let [defaults {:cupboard *cupboard*}
        opts (merge defaults (args-map opts-args))
        pass-through-opts (dissoc opts :cupboard)
        cb (opts :cupboard)]
    (db-env-verify @(cb :cupboard-env) pass-through-opts)))


(defn verify-shelf [& opts-args]
  (let [defaults {:cupboard *cupboard*
                  :shelf-name *default-shelf-name*}
        opts (merge defaults (args-map opts-args))
        pass-through-opts (dissoc opts :cupboard :shelf-name)
        cb (opts :cupboard)
        shelf-name (opts :shelf-name)
        shelf (get-shelf cb shelf-name)]
    ;; verify the shelf itself
    (db-verify (shelf :db) pass-through-opts)
    ;; verify all indices
    (doseq [[_ idx] (merge @(shelf :index-any-dbs) @(shelf :index-unique-dbs))]
      (db-sec-verify idx pass-through-opts))))


(defn verify
  "Very expensive operation. Runs verify on the cupboard environment, every
   shelf, and every index."
  [& opts-args]
  (let [defaults {:cupboard *cupboard*}
        opts (merge defaults (args-map opts-args))
        cb (opts :cupboard)]
    (apply verify-cupboard-env opts-args)
    (doseq [[shelf-name _] @(cb :shelves)]
      (apply verify-shelf (concat [:shelf-name shelf-name] opts-args)))))


(defn modify-env
  "Wrapper for tuning cupboard database environments."
  [& opts-args]
  (let [defaults {:cupboard *cupboard*}
        opts (merge defaults (args-map opts-args))
        pass-through-opts (dissoc opts :cupboard)
        cb (opts :cupboard)]
    (db-env-modify @(cb :cupboard-env) pass-through-opts)))



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
         (defmethod make-instance ~name [struct-type# struct-args#
                                         & instance-kw-raw-args#]
           (let [instance-kw-args# (args-map instance-kw-raw-args#)
                 inst-kw-meta-args# (dissoc instance-kw-args# :txn :save)
                 save-instance# (or (not (contains? instance-kw-args# :save))
                                    (instance-kw-args# :save))
                 inst-kw-save-args# (select-keys instance-kw-args#
                                                 [:cupboard :txn :shelf-name])
                 inst-meta-base# (struct persistence-metadata
                                   (java.util.UUID/randomUUID)
                                   ~idx-uniques ~idx-anys)
                 inst-meta# (merge ~opts inst-kw-meta-args# inst-meta-base#)
                 inst# (with-meta
                         (apply struct (cons struct-type# struct-args#))
                         inst-meta#)]
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
        parent-txn (opts :parent-txn)
        cb (opts :cupboard)
        direct-opts (merge {:txn parent-txn}
                           (dissoc opts :cupboard :parent-txn))]
    (db-txn-begin @(cb :cupboard-env) direct-opts)))


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
  (let [[txn-var opts-args] (cond (empty? args) ['cupboard.core/*txn* args]
                                  (keyword? (first args)) ['cupboard.core/*txn* args]
                                  :else [(first args) (rest args)])
        defaults {:max-attempts 5
                  :retry-delay-msec 10}
        opts (merge defaults (args-map opts-args))
        max-attempts (opts :max-attempts)
        retry-delay-msec (opts :retry-delay-msec)
        direct-opts (dissoc opts :max-attempts :retry-delay-msec)]
    `(let [max-attempts# ~max-attempts
           retry-delay-msec# ~retry-delay-msec
           direct-opts# ~direct-opts]
       ;; XXX: The deadlock retry construct requires explicit
       ;; recursion. Clojure's recur cannot occur inside a catch block, so
       ;; attempting a retry requires an explicit, stack-eating function call.
       (letfn [(attempt-txn# [attempt#]
                 (~(if (= txn-var 'cupboard.core/*txn*) 'binding 'let)
                  [~txn-var (begin-txn direct-opts#)]
                    (try
                     ~@body
                     (when (= @(~txn-var :status) :open)
                       (commit ~txn-var))
                     (catch DeadlockException deadlock#
                       (if (< attempt# max-attempts#)
                           (do
                             (rollback ~txn-var)
                             (Thread/sleep retry-delay-msec#)
                             (attempt-txn# (inc attempt#)))
                           (do
                             (rollback ~txn-var)
                             (throw (RuntimeException. deadlock#)))))
                     (catch Exception other-error#
                       (do
                         (rollback ~txn-var)
                         (throw (RuntimeException. other-error#)))))))]
         (attempt-txn# 1)))))



;;; ----------------------------------------------------------------------------
;;; queries
;;; ----------------------------------------------------------------------------

(defn- db-res->cb-struct
  "Takes a raw result returned from a db routine and converts it into a map with
   metadata like the ones created by make-instance. Unless the :struct argument
   is given, returns a hash-map instance.

   Optional keyword argument:
     :struct --- make the result a struct-map of the given type"
  [[pkey value] shelf & opts-args]
  (when-not (nil? value)
    (let [opts (args-map opts-args)
          struct-type (opts :struct)    ; may be nil
          index-unique-dbs @(shelf :index-unique-dbs)
          index-any-dbs @(shelf :index-any-dbs)
          value-key-set (set (keys value))
          metadata {:primary-key pkey
                    :index-uniques (intersection (set (keys index-unique-dbs))
                                                 value-key-set)
                    :index-anys (intersection (set (keys index-any-dbs))
                                              value-key-set)}
          xvalue (if (and (not (nil? struct-type)) (contains? opts :struct))
                     (apply struct-map (flatten [struct-type (seq value)]))
                     value)]
      (with-meta xvalue metadata))))


(defn- close-cursors [cursors]
  (doseq [cursor cursors]
    (db-cursor-close cursor)))


(defn query-natural-join [clauses cb shelf-name txn lock-mode struct-type]
  (let [cursors (atom [])
        shelf (get-shelf cb shelf-name)
        all-indices (merge @(shelf :index-unique-dbs) @(shelf :index-any-dbs))]
    (try
     ;; open and position all cursors
     (doseq [[_ indexed-slot indexed-value] clauses]
       (let [cursor (db-cursor-open (all-indices indexed-slot) :txn txn)]
         (db-cursor-search cursor indexed-value :exact true :lock-mode lock-mode)
         (swap! cursors conj cursor)))
     ;; join
     (let [jc (db-join-cursor-open @cursors)]
       (letfn [(join-iter []
                 (try
                  (let [res (db-join-cursor-next jc :lock-mode lock-mode)]
                    (if (empty? res)
                        (lazy-seq)
                        (lazy-seq (cons (db-res->cb-struct res shelf :struct struct-type)
                                        (join-iter)))))
                  (catch DatabaseException de
                    (db-join-cursor-close jc)
                    (throw de))))]
         ;; Return both the join cursor and the resulting lazy sequence. A
         ;; consumer of this function (the query macro) should either consume
         ;; the whole sequence or make sure it closes the cursor.
         [jc (join-iter)]))
     ;; close all cursors on clean up
     (finally
      ;; XXX: Must be a separate named function, as Clojure does not support
      ;; iteration inside catch or finally forms.
      (close-cursors @cursors)))))


(defn- determine-dominating-clause [clauses]
  ;; XXX: Clearly room for improvement here. Clauses should be ordered from
  ;; smallest remaining key range to largest, since iterating over the smallest
  ;; dataset puts an upper bound on the total number of iterations required.
  ;; Unfortunately, Berkeley DB JE does not support a Database/getKeyRange
  ;; function which would help determine approximately where in an index a
  ;; specified key value resides. Berkeley DB Core does have this functionality.
  (first clauses))


(defn query-range-join [clauses cb shelf-name txn lock-mode struct-type]
  (let [shelf (get-shelf cb shelf-name)
        all-indices (merge @(shelf :index-unique-dbs) @(shelf :index-any-dbs))
        dominating-clause (determine-dominating-clause clauses)
        [dc-fn-symbol dc-idx dc-val] dominating-clause
        check-fn (fn [entry] ; Make sure entry satisfies all clauses.
                   (every? (fn [[f k v]] (f ((second entry) k) v)) clauses))
        main-cursor (db-cursor-open (all-indices dc-idx) :txn txn)]
    (try
     (let [res (filter check-fn
                       (db-cursor-scan main-cursor
                                       dc-val
                                       :comparison-fn dc-fn-symbol
                                       :lock-mode lock-mode))
           final-res (map #(db-res->cb-struct % shelf :struct struct-type) res)]
       ;; Return both the main cursor and the resulting lazy sequence. A
       ;; consumer of this function (the query macro) should either consume the
       ;; whole sequence or make sure it closes the cursor.
       [main-cursor final-res])
     ;; clean up in case of error
     (catch DatabaseException de
       (db-cursor-close main-cursor)
       (throw de)))))


(defn query-pkey [cb shelf-name txn lock-mode struct-type]
  (let [shelf (get-shelf cb shelf-name)
        cursor (db-cursor-open (shelf :db) :txn txn)]
    (letfn [(scan [prev-res]
              (if (empty? prev-res)
                  (lazy-seq)
                  (lazy-seq (cons prev-res
                                  (scan (db-cursor-next cursor
                                                        :lock-mode lock-mode))))))]
      (try
       (let [res (scan (db-cursor-first cursor))
             final-res (map #(db-res->cb-struct % shelf :struct struct-type) res)]
         [cursor final-res])
       ;; clean up in case of error
       (catch DatabaseException de
         (db-cursor-close cursor)
         (throw de))))))


;; XXX: Revisit this when Clojure's scopes feature comes to life.
;; http://www.assembla.com/spaces/clojure/tickets/2-Scopes
(defmacro query [& args]
  (let [[clauses opts-args] (args-&rest-&keys args)
        defaults {:limit nil
                  :callback identity
                  :cupboard 'cupboard.core/*cupboard*
                  :shelf-name 'cupboard.core/*default-shelf-name*
                  :txn 'cupboard.core/*txn*}
        opts (merge defaults opts-args)
        callback (opts :callback)
        ;; XXX: This check cannot happen against res-clauses, because
        ;; res-clauses only resolves the function it uses at runtime. The query
        ;; macro aims to know which join to perform at macroexpansion time
        ;; (although this may change).
        join-type (cond
                    (empty? clauses) :join-none
                    (every? #(= '= %) (map first clauses)) :join-natural
                    :else :join-range)
        ;; Evaluate the individual elements of each clause. Both easier and more
        ;; correct than working with raw symbols.
        res-clauses `(list ~@(map (fn [[f k v]] `(list ~f ~k ~v)) clauses))
        struct-type (opts :struct)
        limit (opts :limit)
        lock-mode (cond
                    (contains? opts :lock-mode) (opts :lock-mode)
                    (= callback identity) :default
                    :else :read-uncommitted)
        cb (opts :cupboard)
        shelf-name (opts :shelf-name)
        txn (opts :txn)]
    `(let [callback# ~callback
           [cursor# raw-res#]
           ~(condp = join-type
              :join-none `(query-pkey ~cb ~shelf-name ~txn ~lock-mode ~struct-type)
              :join-natural `(query-natural-join ~res-clauses ~cb ~shelf-name
                                                 ~txn ~lock-mode ~struct-type)
              :join-range `(query-range-join ~res-clauses ~cb ~shelf-name
                                             ~txn ~lock-mode ~struct-type))]
       (try
        (let [xres# (map callback# raw-res#)
              limit# ~limit
              lres# (doall (if (nil? limit#)
                               xres#
                               (take limit# xres#)))]
          ;; return the limited result here
          lres#)
        (finally
         (~(condp = join-type
             ;; must use fully-qualified names
             :join-none 'cupboard.bdb.je/db-cursor-close
             :join-natural 'cupboard.bdb.je/db-join-cursor-close
             :join-range 'cupboard.bdb.je/db-cursor-close)
          cursor#))))))



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
        struct-type (opts :struct)      ; may be nil
        cb (opts :cupboard)
        txn (opts :txn)
        shelf-name (opts :shelf-name)
        shelf (get-shelf cb shelf-name)
        index-unique-dbs @(shelf :index-unique-dbs)
        index-any-dbs @(shelf :index-any-dbs)]
    ;; If the index-slot is in :index-uniques, retrieve it and return as is.
    ;; If the index-slot is in :index-anys, retrieve a lazy sequence.
    (cond
      ;; uniques
      (contains? index-unique-dbs index-slot)
      (let [res (check-txn txn
                  (db-sec-get (index-unique-dbs index-slot) indexed-value
                              :txn txn))]
        (db-res->cb-struct res shelf :struct struct-type))
      ;; anys --- cannot use with-db-cursor on lazy sequences
      (contains? index-any-dbs index-slot)
      (query (= index-slot indexed-value)
             :cupboard cb :shelf-name shelf-name :txn txn :struct struct-type)
      ;; not retrieving an indexed slot
      :else (throw (RuntimeException. (str "attempting retrieve by slot "
                                           index-slot ", not indexed on shelf "
                                           (shelf :name)))))))


(defn delete [obj & opts-args]
  (let [defaults {:cupboard *cupboard*
                  :shelf-name *default-shelf-name*
                  :txn *txn*}
        pmeta (meta obj)
        opts (merge defaults (args-map opts-args))
        cb (opts :cupboard)
        txn (opts :txn)
        shelf (get-shelf cb (opts :shelf-name))]
    (let [res (check-txn txn
                (db-delete (shelf :db) (pmeta :primary-key) :txn txn))]
      (when-not (= res OperationStatus/SUCCESS)
        (throw (RuntimeException. (str "failed to delete " obj)))))))


(defn passoc!
  "Just like clojure.core/assoc, but works on objects defined with defpersist
   and created with make-instance. Automatically saves modifications.
   Two forms: (passoc! obj new-key new-value & options)
              (passoc! obj [k1 v1 k2 v2 ...] & options)"
  [obj & args]
  (if (sequential? (first args))
      ;; long assoc form, permitting multiple key-value pairs
      (let [[kv-pairs & opts-args] args
            opts (args-map opts-args)]
        (save (apply assoc (cons obj kv-pairs)) opts))
      ;; short assoc form
      (let [[key value & opts-args] args
            opts (args-map opts-args)]
        (save (assoc obj key value) opts))))


(defn pdissoc!
  "Just like clojure.core/dissoc, but works on objects defined with defpersist
   and created with make-instance. Automatically saves modifications.
   Two forms: (pdissoc! obj key & options)
              (pdissoc! obj [k1 k2 ...] & options)"
  [obj & args]
  (if (sequential? (first args))
      ;; long dissoc form, permitting multiple key-value pairs
      (let [[keys & opts-args] args
            opts (args-map opts-args)]
        (save (apply dissoc (cons obj keys)) opts))
      ;; short dissoc form
      (let [[key & opts-args] args
            opts (args-map opts-args)]
        (save (dissoc obj key) opts))))
