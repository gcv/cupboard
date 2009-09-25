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
  (let [p1 (cb/make-instance president ["gw" "George" "Washington" 57] :save false)
        p2 (cb/make-instance president ["ja" "John" "Adams" 62] :save false)
        p3 (cb/make-instance president ["tj" "Thomas" "Jefferson" 58] :save false)
        p4 (cb/make-instance president ["jm" "James" "Madison" 58] :save false)]
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

      (testing "making an empty cupboard and checking its state"
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
        (is (not (-> (@(@cb :shelves) *default-shelf-name*) :db :sorted-duplicates))))

      (testing "writing something to the default shelf"
        (cb/make-instance president ["gw" "George" "Washington" 57] :cupboard @cb)
        (verify-shelf *default-shelf-name*))

      (testing "writing something to a different shelf"
        (cb/make-instance president ["ja" "John" "Adams" 62] :cupboard @cb :shelf-name "presidents")
        (verify-shelf "presidents"))

      (testing "closing cupboard"
        (close-cupboard @cb)
        (= (nil? @(@cb :cupboard-env)))
        (= (empty? @(@cb :shelves-db)))
        (= (empty? @(@cb :shelves))))

      (testing "reopening cupboard, and verifying correctness of reopened state"
        (reset! cb (open-cupboard *cupboard-path*))
        (verify-shelf *default-shelf-name*)
        (verify-shelf "presidents"))

      (testing "deleting shelf"
        (remove-shelf "presidents" :cupboard @cb)
        (verify-shelf *default-shelf-name*)
        (is (not (contains? @(@cb :shelves) "presidents")))
        (is (not (contains? (list-shelves :cupboard @cb) "presidents"))))

      (testing "closing and reopening cupboard, and verifying correctness"
        (close-cupboard @cb)
        (reset! cb (open-cupboard *cupboard-path*))
        (verify-shelf *default-shelf-name*)
        (is (not (contains? @(@cb :shelves) "presidents"))))

      (testing "checking invalid shelf names"
        (is (thrown? RuntimeException
                     (cb/make-instance president ["tj" "Thomas" "Jefferson" 58]
                                       :cupboard cb :shelf-name "invalid:name")))
        (is (thrown? RuntimeException
                     (cb/make-instance president ["tj" "Thomas" "Jefferson" 58]
                                       :cupboard cb :shelf-name *shelves-db-name*))))

      (close-cupboard @cb)))

  (testing "check the correctness of the cupboard databases"
    (db-core/with-db-env [env *cupboard-path*]
      (let [idx-name-age (str *default-shelf-name* :age)
            idx-name-bank-acct (str *default-shelf-name* :bank-acct)
            idx-name-first-name (str *default-shelf-name* :first-name)
            idx-name-last-name (str *default-shelf-name* :last-name)
            idx-name-login (str *default-shelf-name* :login)]
        (testing "checking environment"
          (is (= (set (.getDatabaseNames @(env :env-handle)))
                 #{*shelves-db-name* *default-shelf-name*
                   idx-name-age idx-name-bank-acct idx-name-first-name
                   idx-name-last-name idx-name-login})))
        (testing "checking _shelves"
          (db-core/with-db [shelves-db env *shelves-db-name*]
            (db-core/with-db-cursor [cur1 shelves-db]
              (is (= (db-cursor-first cur1) [*default-shelf-name* {}]))
              (is (= (db-cursor-next cur1) [idx-name-age {:sorted-duplicates true}]))
              (is (= (db-cursor-next cur1) [idx-name-bank-acct {:sorted-duplicates false}]))
              (is (= (db-cursor-next cur1) [idx-name-first-name {:sorted-duplicates true}]))
              (is (= (db-cursor-next cur1) [idx-name-last-name {:sorted-duplicates true}]))
              (is (= (db-cursor-next cur1) [idx-name-login {:sorted-duplicates false}]))
              (is (= (db-cursor-next cur1) [])))))))))


(deftest basics
  (let [cupboard-location (make-temp-dir)
        p1 (atom nil)
        p2 (atom nil)
        p3 (atom nil)
        p4 (atom nil)]

    (testing "default cb/*cupboard*"
      (try
       (cb/with-open-cupboard [cupboard-location]
         (reset! p1 (cb/make-instance president ["gw" "George" "Washington" 57]))
         (reset! p2 (cb/make-instance president ["ja" "John" "Adams" 62]))
         (reset! p3 (cb/make-instance president ["tj" "Thomas" "Jefferson" 58]))
         (reset! p4 (cb/make-instance president ["jm" "James" "Madison" 58])))

       (testing "ability to build a struct-map on retrieve"
         (cb/with-open-cupboard [cupboard-location]
           (is (= @p1 (cb/retrieve :login "gw")))
           (let [sp1 (cb/retrieve :login "gw" :struct president)]
             (is (= (type sp1) clojure.lang.PersistentStructMap))
             (is (= (meta sp1) (meta @p1))))))

       (testing "plain hash-map retrieval"
         (cb/with-open-cupboard [cupboard-location]
           (is (= @p1 (cb/retrieve :login "gw")))
           (is (= @p2 (cb/retrieve :login "ja" :cupboard cb/*cupboard*)))
           (is (= @p3 (cb/retrieve :login "tj")))
           (is (= @p4 (cb/retrieve :login "jm")))
           (testing ":any index retrieval"
             (is (= (set (cb/retrieve :age 58)) #{@p4 @p3})))
           (testing "deletion"
             (cb/delete @p2)
             (is (nil? (cb/retrieve :login "ja"))))))
         (finally
          (rmdir-recursive cupboard-location))))

    (testing "explicitly bound cupboard"
      (try
       (cb/with-open-cupboard [cb cupboard-location]
         (reset! p1 (cb/make-instance president ["gw" "George" "Washington" 57] :cupboard cb))
         (reset! p2 (cb/make-instance president ["ja" "John" "Adams" 62] :cupboard cb))
         (reset! p3 (cb/make-instance president ["tj" "Thomas" "Jefferson" 58] :cupboard cb))
         (reset! p4 (cb/make-instance president ["jm" "James" "Madison" 58] :cupboard cb)))
       (cb/with-open-cupboard [cb cupboard-location]
         (is (= @p1 (cb/retrieve :login "gw" :cupboard cb)))
         (is (= @p2 (cb/retrieve :login "ja" :cupboard cb)))
         (is (= @p3 (cb/retrieve :login "tj" :cupboard cb)))
         (is (= @p4 (cb/retrieve :login "jm" :cupboard cb)))
         (is (thrown? NullPointerException (cb/retrieve :login "jm"))))
       (finally
        (rmdir-recursive cupboard-location))))))


(deftest transaction-basics
  (cb/with-open-cupboard [*cupboard-path*]
    (let [p1 {:login "gw" :first-name "George" :last-name "Washington" :age 57 :bank-acct nil}]

      (testing "basic transactions"
        (cb/with-txn []
          (cb/make-instance president ["gw" "George" "Washington" 57])
          (is (= (cb/retrieve :login "gw") p1))
          (cb/rollback)
          (is (thrown-with-msg?
                RuntimeException #".*non-open transaction"
                (cb/make-instance president ["ja" "John" "Adams" 62]))))
        (is (empty? (cb/retrieve :login "gw")))
        (is (empty? (cb/retrieve :login "ja")))
        (cb/with-txn [:write-no-sync true]
          (cb/make-instance president ["gw" "George" "Washington" 57])
          (cb/commit)
          (is (thrown-with-msg?
                RuntimeException #".*non-open transaction"
                (cb/make-instance president ["ja" "John" "Adams" 62]))))
        (is (empty? (cb/retrieve :login "ja")))
        (is (= (cb/retrieve :login "gw") p1)))

      (testing "transactional shelf removal"
        (cb/make-instance president ["aj" "Andrew" "Johnson"] :shelf-name "presidents")
        (cb/with-txn []
          (is (= (cb/retrieve :login "aj" :shelf-name "presidents")
                 {:login "aj" :first-name "Andrew" :last-name "Johnson"
                  :bank-acct nil :age nil}))
          (cb/remove-shelf "presidents")
          (is (not (some #(= % "presidents") (list-shelves))))
          (cb/rollback))
        (is (some #(= % "presidents") (list-shelves)))))))


(deftest transaction-binding
  (cb/with-open-cupboard [*cupboard-path*]
    (testing "lexically bound transaction"
      (cb/with-txn [txn1]
        (cb/make-instance president ["gw" "George" "Washington" 57] :txn txn1)
        (cb/rollback txn1))
      (is (empty? (cb/retrieve :login "gw"))))))


(deftest deadlocks
  ;; Cannot use with-open-cupboard because Clojure's dynamic variables do not
  ;; propagate to child threads.
  (let [cb (open-cupboard *cupboard-path*)]
    (try
     (let [gw (cb/make-instance president ["gw" "George" "Washington" 57] :cupboard cb)
           ja (cb/make-instance president ["ja" "John" "Adams" 62] :cupboard cb)
           done-1 (atom false)
           done-2 (atom false)]

       (testing "deadlock resolution, both threads commit"
         (.start (Thread. (fn []
                            (with-txn [:cupboard cb :max-attempts 2 :retry-delay-msec 10]
                              (cb/passoc! gw :bank-acct 1 :cupboard cb)
                              (Thread/sleep 5)
                              (cb/passoc! ja :bank-acct 2 :cupboard cb))
                            (reset! done-1 true))))
         (.start (Thread. (fn []
                            (with-txn [:cupboard cb :max-attempts 2 :retry-delay-msec 50]
                              (cb/passoc! ja :bank-acct 3 :cupboard cb)
                              (Thread/sleep 5)
                              (cb/passoc! gw :bank-acct 4 :cupboard cb))
                            (reset! done-2 true))))
         ;; wait for threads to complete
         (loop [i 0]
           (when-not (and @done-1 @done-2)
             (Thread/sleep 100)
             (recur (inc i))))
         ;; The first thread has a shorter retry delay, so it should win the race.
         (is (= (cb/retrieve :login "gw" :cupboard cb) (assoc gw :bank-acct 4)))
         (is (= (cb/retrieve :login "ja" :cupboard cb) (assoc ja :bank-acct 3))))

       (testing "deadlock resolution, one thread rolls back permanently"
         (reset! done-1 false)
         (reset! done-2 false)
         (.start (Thread. (fn []
                            (with-txn [:cupboard cb :max-attempts 2 :retry-delay-msec 100]
                              (cb/passoc! gw :bank-acct 5 :cupboard cb)
                              (Thread/sleep 100)
                              (cb/passoc! ja :bank-acct 6 :cupboard cb))
                            (reset! done-1 true))))
         (.start (Thread. (fn []
                            (is (thrown? RuntimeException
                                  (try
                                   (with-txn [:cupboard cb :max-attempts 1]
                                     (cb/passoc! ja :bank-acct 7 :cupboard cb)
                                     (Thread/sleep 10)
                                     (cb/passoc! gw :bank-acct 8 :cupboard cb))
                                   (finally
                                    (reset! done-2 true))))))))
         ;; wait for threads to complete
         (loop [i 0]
           (when-not (and @done-1 @done-2)
             (Thread/sleep 100)
             (recur (inc i))))
         ;; Only the first thread should commit here.
         (is (= (cb/retrieve :login "gw" :cupboard cb) (assoc gw :bank-acct 5)))
         (is (= (cb/retrieve :login "ja" :cupboard cb) (assoc ja :bank-acct 6)))))

     (finally
      (close-cupboard cb)))))


(deftest passoc!-pdissoc!
  (cb/with-open-cupboard [*cupboard-path*]
    (let [date-gw (localdate "1732-02-22")
          gw1 {:login "gw" :first-name "George" :last-name "Washington"
               :age 57 :bank-acct nil}
          gw2 {:login "gw" :first-name "George" :last-name "Washington"
               :age 57 :bank-acct 1}
          gw3 {:login "gw" :first-name "George" :last-name "Washington"
               :age 57 :bank-acct 1 :birthday date-gw}
          date-ja (localdate "1735-10-30")
          ja1 {:login "ja" :first-name "John" :last-name "Adams" :age 62 :bank-acct nil}
          ja2 {:login "ja" :first-name "John" :last-name "Adams" :age 62 :bank-acct 2}
          ja3 {:login "ja" :first-name "John" :last-name "Adams" :age 62 :bank-acct 2
               :birthday date-ja}
          date-tj (localdate "1743-04-13")
          tj1 {:login "tj" :first-name "Thomas" :last-name "Jefferson" :age 58 :bank-acct nil}
          tj2 {:login "tj" :first-name "Thomas" :last-name "Jefferson" :age 58 :bank-acct 3}
          tj3 {:login "tj" :first-name "Thomas" :last-name "Jefferson" :age 58 :bank-acct 3
               :birthday date-tj}]

      (testing "simple passoc!-pdissoc! operations"
        (let [p (atom (cb/make-instance president ["gw" "George" "Washington" 57]))]
          (is (= (cb/retrieve :login "gw") gw1))
          (reset! p (cb/passoc! @p :bank-acct 1))
          (is (= (cb/retrieve :login "gw") gw2))
          (reset! p (cb/passoc! @p :birthday date-gw))
          (is (= (cb/retrieve :login "gw") gw3))
          (reset! p (cb/pdissoc! @p :birthday))
          (is (= (cb/retrieve :login "gw") gw2))))

      (testing "passoc!-pdissoc! operations on non-default shelves"
        (let [p (atom (cb/make-instance president ["ja" "John" "Adams" 62]
                                        :shelf-name "presidents"))]
          (is (= (cb/retrieve :login "ja" :shelf-name "presidents") ja1))
          (reset! p (cb/passoc! @p :bank-acct 2))
          (is (= (cb/retrieve :login "ja" :shelf-name "presidents") ja2))
          (reset! p (cb/passoc! @p :birthday date-ja))
          (is (= (cb/retrieve :login "ja" :shelf-name "presidents") ja3))
          (reset! p (cb/pdissoc! @p :birthday))
          (is (= (cb/retrieve :login "ja" :shelf-name "presidents") ja2))))

      (testing "passoc!-pdissoc! operations with multiple operands"
        (let [p (atom (cb/make-instance president ["tj" "Thomas" "Jefferson" 58]))]
          (is (= (cb/retrieve :login "tj") tj1))
          (reset! p (cb/passoc! @p [:bank-acct 3 :birthday date-tj]))
          (is (= (cb/retrieve :login "tj") tj3))
          (reset! p (cb/passoc! @p [:nonce1 1 :nonce2 2]))
          (reset! p (cb/pdissoc! @p [:birthday :nonce1 :nonce2]))
          (is (= (cb/retrieve :login "tj") tj2)))))))


(deftest queries
  (cb/with-open-cupboard [*cupboard-path*]
    (let [p1 (cb/make-instance president ["gw" "George" "Washington" 57])
          p2 (cb/make-instance president ["ja" "John" "Adams" 62])
          p3 (cb/make-instance president ["tj" "Thomas" "Jefferson" 58])
          p4 (cb/make-instance president ["jm1" "James" "Madison" 58])
          p5 (cb/make-instance president ["jm2" "James" "Monroe" 59])
          p6 (cb/make-instance president ["jqa" "John" "Adams" 58])
          p7 (cb/make-instance president ["aj" "Andrew" "Jackson" 62])
          p8 (cb/make-instance president ["mvb" "Martin" "Van Buren" 55])
          p9 (cb/make-instance president ["whh" "William" "Harrison" 68])
          p10 (cb/make-instance president ["jt" "John" "Tyler" 51])]

      (testing "basic one-clause query operations"
        (is (= (set (cb/query (= :login "gw"))) #{p1}))
        (is (= (set (cb/query (= :login "aj"))) #{p7}))
        (is (= (set (cb/query (= :age 57))) #{p1}))
        (is (empty? (cb/query (= :age 57) (= :age 62))))
        (is (= (set (cb/query (= :age 62))) #{p2 p7}))
        (is (= (set (cb/query (<= :age 55))) #{p8 p10}))
        (is (= (set (cb/query (< :age 55))) #{p10}))
        (is (= (set (cb/query (> :age 60))) #{p2 p7 p9})))

      (testing "queries with multiple clauses"
        (is (= (set (cb/query (< :age 60) (starts-with :first-name "J"))) #{p4 p5 p6 p10}))
        (is (= (set (cb/query (< :age 60) (starts-with :first-name "Ja"))) #{p4 p5}))
        (is (= (set (cb/query (< :age 60) (= :first-name "John"))) #{p6 p10}))
        (is (= (set (cb/query (= :first-name "John"))) #{p2 p6 p10}))
        (is (= (count (cb/query (< :age 60) (starts-with :first-name "J") :limit 2)) 2)))

      (testing "destructive callbacks"
        (cb/query (< :age 60) (= :first-name "John")
                  :callback #(cb/passoc! % :first-name "Jack"))
        (is (= (cb/retrieve :login "ja") p2))
        (is (= (cb/retrieve :login "jqa") (assoc p6 :first-name "Jack"))))

      (testing "making sure natural joins are used wherever possible"
        (let [q (macroexpand-1
                 '(cb/query (= :age 58) (= :last-name "Adams")
                            :callback #(cb/passoc! % :first-name "John Quincy")))]
          (is (= (first (first (rest (rest (rest (first (rest q)))))))
                 'cupboard/query-natural-join)))
        (cb/query (= :age 58) (= :last-name "Adams")
                  :callback #(cb/passoc! % :first-name "John Quincy"))
        (is (= (cb/retrieve :login "jqa") (assoc p6 :first-name "John Quincy"))))

      (testing "delete as a callback"
        (cb/query (= :age 58) :callback cb/delete)
        (is (nil? (cb/retrieve :login "tj")))
        (is (nil? (cb/retrieve :login "jm1")))
        (is (nil? (cb/retrieve :login "jqa"))))

      (testing "making sure that :struct applied to query works"
        (let [everyone (cb/query (> :age 50) :struct president)]
          (is (> (count everyone) 0))
          (is (every? #(= (type %) clojure.lang.PersistentStructMap) everyone)))))))


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
