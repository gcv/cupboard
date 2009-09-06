(ns test.cupboard
  (:use [clojure.contrib test-is])
  (:use [cupboard utils])
  (:use [cupboard.db-core :as db-core])
  (:use [cupboard :as cb]))



;;; ----------------------------------------------------------------------------
;;; fixtures
;;; ----------------------------------------------------------------------------

(declare *cupboard-path*)


(defn fixture-cupboard-path [f]
  (binding [*cupboard-path* (.getAbsolutePath (make-temp-dir))]
    (f)
    (rmdir-recursive *cupboard-path*)))


(use-fixtures :each fixture-cupboard-path)



;;; ----------------------------------------------------------------------------
;;; tests
;;; ----------------------------------------------------------------------------

(cb/defpersist president
  ((:login :index :unique)
   (:first-name :index :any)
   (:last-name :index :any)
   (:age :index :any)
   (:bank-acct :index :unique)))


(deftest persistent-structures
  (let [p1 (cb/make-instance president :save false "gw" "George" "Washington" 57)
        p2 (cb/make-instance president :save false "ja" "John" "Adams" 62)
        p3 (cb/make-instance president :save false "tj" "Thomas" "Jefferson" 58)
        p4 (cb/make-instance president :save false "jm" "James" "Madison" 58)]
    (is (= (p1 :login) "gw"))
    (is (= (p2 :first-name) "John"))
    (is (= (p3 :age) 58))
    (is (nil? (p4 :bank-acct)))
    (is (= ((meta p2) :index-uniques) #{:login :bank-acct}))
    (is (= ((meta p2) :index-anys) #{:first-name :last-name :age}))))


(deftest cupboard-basics
  (let [cb (atom nil)]
    (letfn [(verify-shelf [shelf-name]
              ;; check :login index
              (is (contains? @((@(@cb :shelves) shelf-name) :index-unique-dbs) :login))
              (is (not (-> @((@(@cb :shelves) shelf-name) :index-unique-dbs)
                           :login :sorted-duplicates)))
              (is (= (-> @((@(@cb :shelves) shelf-name) :index-unique-dbs) :login :name)
                     (str shelf-name :login)))
              ;; check :bank-acct index
              (is (contains? @((@(@cb :shelves) shelf-name) :index-unique-dbs)
                             :bank-acct))
              (is (not (-> @((@(@cb :shelves) shelf-name) :index-unique-dbs)
                           :bank-acct :sorted-duplicates)))
              ;; check :first-name index
              (is (contains? @((@(@cb :shelves) shelf-name) :index-any-dbs) :first-name))
              (is (-> @((@(@cb :shelves) shelf-name) :index-any-dbs)
                      :first-name :sorted-duplicates))
              (is (= (-> @((@(@cb :shelves) shelf-name) :index-any-dbs)
                         :first-name :name)
                     (str shelf-name :first-name)))
              ;; check :last-name index
              (is (contains? @((@(@cb :shelves) shelf-name) :index-any-dbs) :last-name))
              (is (-> @((@(@cb :shelves) shelf-name) :index-any-dbs)
                      :last-name :sorted-duplicates))
              ;; check :age index
              (is (contains? @((@(@cb :shelves) shelf-name) :index-any-dbs) :age))
              (is (-> @((@(@cb :shelves) shelf-name) :index-any-dbs)
                      :age :sorted-duplicates)))]

      ;; make an empty cupboard and check its state
      (reset! cb (open-cupboard *cupboard-path*))
      (is (not (nil? @(@cb :cupboard-env))))
      (is (not (nil? @(@cb :shelves-db))))
      (is (not (nil? @(@cb :shelves))))
      (is (not (@(@cb :shelves-db) :sorted-duplicates)))
      (is (= (count @(@cb :shelves)) 1))
      (is (= (@(@cb :shelves-db) :name) *shelves-db-name*))
      (is (contains? @(@cb :shelves) *default-shelf-name*))
      (is (empty? @((@(@cb :shelves) *default-shelf-name*) :index-unique-dbs)))
      (is (empty? @((@(@cb :shelves) *default-shelf-name*) :index-any-dbs)))
      (is (not (-> (@(@cb :shelves) *default-shelf-name*) :db :sorted-duplicates)))

      ;; write something to the default shelf
      (cb/make-instance president "gw" "George" "Washington" 57 :cupboard @cb)
      (verify-shelf *default-shelf-name*)
      ;; write something to a different shelf
      (cb/make-instance president "ja" "John" "Adams" 62 :cupboard @cb :shelf-name "presidents")
      (verify-shelf "presidents")

      ;; close cupboard
      (close-cupboard @cb)
      (= (nil? @(@cb :cupboard-env)))
      (= (empty? @(@cb :shelves-db)))
      (= (empty? @(@cb :shelves)))

      ;; reopen cupboard, and verify correctness of reopened state
      (reset! cb (open-cupboard *cupboard-path*))
      (verify-shelf *default-shelf-name*)
      (verify-shelf "presidents")

      ;; delete shelf
      (remove-shelf "presidents" :cupboard @cb)
      (verify-shelf *default-shelf-name*)
      (is (not (contains? @(@cb :shelves) "presidents")))
      (is (not (contains? (list-shelves :cupboard @cb) "presidents")))

      ;; close and reopen cupboard, and verify correctness of reopened state
      (close-cupboard @cb)
      (reset! cb (open-cupboard *cupboard-path*))
      (verify-shelf *default-shelf-name*)
      (is (not (contains? @(@cb :shelves) "presidents")))

      ;; check invalid shelf names
      (is (thrown? RuntimeException
                   (cb/make-instance president "tj" "Thomas" "Jefferson" 58
                                     :cupboard cb :shelf-name "invalid:name")))
      (is (thrown? RuntimeException
                   (cb/make-instance president "tj" "Thomas" "Jefferson" 58
                                     :cupboard cb :shelf-name *shelves-db-name*)))

      (close-cupboard @cb)))

  ;; check the correctness of the cupboard databases
  (db-core/with-db-env [env *cupboard-path*]
    (let [idx-name-age (str *default-shelf-name* :age)
          idx-name-bank-acct (str *default-shelf-name* :bank-acct)
          idx-name-first-name (str *default-shelf-name* :first-name)
          idx-name-last-name (str *default-shelf-name* :last-name)
          idx-name-login (str *default-shelf-name* :login)]
      ;; check environment
      (is (= (set (.getDatabaseNames @(env :env-handle)))
             #{*shelves-db-name* *default-shelf-name*
               idx-name-age idx-name-bank-acct idx-name-first-name
               idx-name-last-name idx-name-login}))
      ;; check _shelves
      (db-core/with-db [shelves-db env *shelves-db-name*]
        (db-core/with-db-cursor [cur1 shelves-db]
          (is (= (db-cursor-first cur1) [*default-shelf-name* {}]))
          (is (= (db-cursor-next cur1) [idx-name-age {:sorted-duplicates true}]))
          (is (= (db-cursor-next cur1) [idx-name-bank-acct {:sorted-duplicates false}]))
          (is (= (db-cursor-next cur1) [idx-name-first-name {:sorted-duplicates true}]))
          (is (= (db-cursor-next cur1) [idx-name-last-name {:sorted-duplicates true}]))
          (is (= (db-cursor-next cur1) [idx-name-login {:sorted-duplicates false}]))
          (is (= (db-cursor-next cur1) [])))))))


(deftest basics
  (let [cupboard-location (make-temp-dir)
        p1 (atom nil)
        p2 (atom nil)
        p3 (atom nil)
        p4 (atom nil)]
    ;; default cb/*cupboard*
    (try
     (cb/with-open-cupboard [cupboard-location]
       (reset! p1 (cb/make-instance president "gw" "George" "Washington" 57))
       (reset! p2 (cb/make-instance president "ja" "John" "Adams" 62))
       (reset! p3 (cb/make-instance president "tj" "Thomas" "Jefferson" 58))
       (reset! p4 (cb/make-instance president "jm" "James" "Madison" 58)))
     (cb/with-open-cupboard [cupboard-location]
       (is (= @p1 (cb/retrieve :login "gw")))
       (is (= @p2 (cb/retrieve :login "ja" :cupboard cb/*cupboard*)))
       (is (= @p3 (cb/retrieve :login "tj")))
       (is (= @p4 (cb/retrieve :login "jm")))
       ;; test :any index retrieval
       (is (= (set (cb/retrieve :age 58)) #{@p4 @p3})))
     (finally
      (rmdir-recursive cupboard-location)))
    ;; explicitly bound cupboard
    (try
     (cb/with-open-cupboard [cb cupboard-location]
       (reset! p1 (cb/make-instance president "gw" "George" "Washington" 57 :cupboard cb))
       (reset! p2 (cb/make-instance president "ja" "John" "Adams" 62 :cupboard cb))
       (reset! p3 (cb/make-instance president "tj" "Thomas" "Jefferson" 58 :cupboard cb))
       (reset! p4 (cb/make-instance president "jm" "James" "Madison" 58 :cupboard cb)))
     (cb/with-open-cupboard [cb cupboard-location]
       (is (= @p1 (cb/retrieve :login "gw" :cupboard cb)))
       (is (= @p2 (cb/retrieve :login "ja" :cupboard cb)))
       (is (= @p3 (cb/retrieve :login "tj" :cupboard cb)))
       (is (= @p4 (cb/retrieve :login "jm" :cupboard cb)))
       (is (thrown? NullPointerException (cb/retrieve :login "jm"))))
     (finally
      (rmdir-recursive cupboard-location)))))


(deftest transaction-basics
  (let [p1 {:login "gw" :first-name "George" :last-name "Washington" :age 57 :bank-acct nil}]
    (cb/with-open-cupboard [*cupboard-path*]
      ;; barebones operations
      (testing "basic transactions"
        (cb/with-txn []
          (cb/make-instance president "gw" "George" "Washington" 57)
          (is (= (cb/retrieve :login "gw") p1))
          (cb/rollback)
          (is (thrown-with-msg?
                RuntimeException #".*non-open transaction"
                (cb/make-instance president "ja" "John" "Adams" 62))))
        (is (empty? (cb/retrieve :login "gw")))
        (is (empty? (cb/retrieve :login "ja")))
        (cb/with-txn [:write-no-sync true]
          (cb/make-instance president "gw" "George" "Washington" 57)
          (cb/commit)
          (is (thrown-with-msg?
                RuntimeException #".*non-open transaction"
                (cb/make-instance president "ja" "John" "Adams" 62))))
        (is (empty? (cb/retrieve :login "ja")))
        (is (= (cb/retrieve :login "gw") p1)))
      ;; transactional remove-shelf
      (testing "transactional shelf removal"
        (cb/make-instance president "aj" "Andrew" "Johnson" :shelf-name "presidents")
        (cb/with-txn []
          (is (= (cb/retrieve :login "aj" :shelf-name "presidents")
                 {:login "aj" :first-name "Andrew" :last-name "Johnson" :bank-acct nil :age nil}))
          (cb/remove-shelf "presidents")
          (is (not (some #(= % "presidents") (list-shelves))))
          (cb/rollback))
        (is (some #(= % "presidents") (list-shelves)))))))


(deftest transaction-binding
  (cb/with-open-cupboard [*cupboard-path*]
    (testing "lexically bound transaction"
      (cb/with-txn [txn1]
        (cb/make-instance president "gw" "George" "Washington" 57 :txn txn1)
        (cb/rollback txn1))
      (is (empty? (cb/retrieve :login "gw"))))))


(deftest assoc*-dissoc*
  (let [date-gw (iso8601->date "1732-02-22 00:00:00Z")
        gw1 {:login "gw" :first-name "George" :last-name "Washington"
             :age 57 :bank-acct nil}
        gw2 {:login "gw" :first-name "George" :last-name "Washington"
             :age 57 :bank-acct 1}
        gw3 {:login "gw" :first-name "George" :last-name "Washington"
             :age 57 :bank-acct 1 :birthday date-gw}
        date-ja (iso8601->date "1735-10-30 00:00:00Z")
        ja1 {:login "ja" :first-name "John" :last-name "Adams" :age 62 :bank-acct nil}
        ja2 {:login "ja" :first-name "John" :last-name "Adams" :age 62 :bank-acct 2}
        ja3 {:login "ja" :first-name "John" :last-name "Adams" :age 62 :bank-acct 2
             :birthday date-ja}]
    (cb/with-open-cupboard [*cupboard-path*]
      (testing "simple assoc*-dissoc* operations"
        (let [p (atom (cb/make-instance president "gw" "George" "Washington" 57))]
          (is (= (cb/retrieve :login "gw") gw1))
          (reset! p (cb/assoc* @p :bank-acct 1))
          (is (= (cb/retrieve :login "gw") gw2))
          (reset! p (cb/assoc* @p :birthday date-gw))
          (is (= (cb/retrieve :login "gw") gw3))
          (reset! p (cb/dissoc* @p :birthday))
          (is (= (cb/retrieve :login "gw") gw2))))
      (testing "assoc*-dissoc* operations non-default shelves"
        (let [p (atom (cb/make-instance president "ja" "John" "Adams" 62
                                        :shelf-name "presidents"))]
          (is (= (cb/retrieve :login "ja" :shelf-name "presidents") ja1))
          (reset! p (cb/assoc* @p :bank-acct 2))
          (is (= (cb/retrieve :login "ja" :shelf-name "presidents") ja2))
          (reset! p (cb/assoc* @p :birthday date-ja))
          (is (= (cb/retrieve :login "ja" :shelf-name "presidents") ja3))
          (reset! p (cb/dissoc* @p :birthday))
          (is (= (cb/retrieve :login "ja" :shelf-name "presidents") ja2)))))))


;; (deftest demo
;;   (cb/with-open-cupboard [*cb* "/mnt/cupboard-raid-10"]
;;     (cb/with-transaction [*txn* :cb *cb*] ; optional keyword argument; can all this be optional?
;;       (let [p1 (cb/make-instance president "gw" "George" "Washington" 57)
;;             ;; :txn and :cb are implicit and optional. cb/make-instance
;;             ;; saves the created map to the cupboard automatically. The
;;             ;; returned map also contains all the metadata necessary to
;;             ;; manipulate the map and cleanly persist it.
;;             p2 (cb/make-instance president "ja" "John" "Adams" 62)
;;             p3 (cb/make-instance president "tj" "Thomas" "Jefferson" 58)
;;             p4 (cb/make-instance president "jm" "James" "Madison" 58)]
;;         (let [p1-aug (assoc p1 :date-birth (iso8601->date "1732-02-22 00:00:00Z"))]
;;           ;; After modifying p1 and making a new map with an additional
;;           ;; piece of data, save the change in the cupboard. :txn and
;;           ;; :cb are implicit and optional. Note that this modification
;;           ;; happens in the same transaction as the creation, so should
;;           ;; roll into a single write.
;;           (cb/save p1-aug))))
;;     ;; The transaction has now committed.
;;     (cb/with-transaction
;;       (cb/save (assoc p2 :date-birth "1735-10-30"))
;;       ;; No, that's wrong. :date-birth should contain a java.util.Date
;;       ;; object. Roll back the transaction.
;;       (cb/rollback))
;;     ;; Load an entry by an index value:
;;     (let [gw (cb/retrieve :login "gw")]
;;       (println gw))
;;     ;; Show off querying:
;;     ;; XXX: This is not quite right. Which shelf should the given indices refer to?
;;     ;; How would inter-shelf joins work?
;;     (let [first-name-j   (cb/query (cb/>= :first-name "J"))
;;           under-60       (cb/query (cb/< :age 60))
;;           under-60-and-j (cb/query (cb/< :age 60) (cb/>= :first-name "J"))
;;           under-60-or-j  (cb/query (cb/or (cb/< :age 60) (cb/>= :first-name "J")))
;;           exactly-62     (cb/query (cb/= :age 62))]
;;       ;; cb/query returns lazy sequences. How would cursors open on
;;       ;; them close when they go out of scope unexhausted, I wonder?
;;       (println under-60-and-j))))
