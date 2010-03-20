(ns test.cupboard.core
  (:use [clojure.contrib test-is])
  (:use cupboard.core cupboard.utils)
  (:require [cupboard.bdb.je :as je]))



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

(defpersist president
  ((:login :index :unique)
   (:first-name :index :any)
   (:last-name :index :any)
   (:age :index :any)
   (:bank-acct :index :unique)))


(deftest persistent-structures
  (let [p1 (make-instance president ["gw" "George" "Washington" 57] :save false)
        p2 (make-instance president ["ja" "John" "Adams" 62] :save false)
        p3 (make-instance president ["tj" "Thomas" "Jefferson" 58] :save false)
        p4 (make-instance president ["jm" "James" "Madison" 58] :save false)]
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
        (make-instance president ["gw" "George" "Washington" 57] :cupboard @cb)
        (verify-shelf *default-shelf-name*))

      (testing "writing something to a different shelf"
        (make-instance president ["ja" "John" "Adams" 62] :cupboard @cb :shelf-name "presidents")
        (verify-shelf "presidents"))

      (testing "closing cupboard"
        (close-cupboard @cb)
        (= (nil? @(@cb :cupboard-env)))
        (= (empty? @(@cb :shelves-db)))
        (= (empty? @(@cb :shelves))))

      (testing "reopening cupboard read-only, and verifying correctness of reopened state"
        (reset! cb (open-cupboard *cupboard-path* :read-only true))
        (verify-shelf *default-shelf-name*)
        (verify-shelf "presidents")
        (is (thrown-with-msg? RuntimeException #".*[Rr]ead.+[Oo]nly.*"
              (query (= :login "gw")
                     :callback #(delete % :cupboard @cb)
                     :cupboard @cb))))

      (testing "closing cupboard again"
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
                     (make-instance president ["tj" "Thomas" "Jefferson" 58]
                                       :cupboard cb :shelf-name "invalid:name")))
        (is (thrown? RuntimeException
                     (make-instance president ["tj" "Thomas" "Jefferson" 58]
                                       :cupboard cb :shelf-name *shelves-db-name*))))

      (close-cupboard @cb)))

  (testing "check the correctness of the cupboard databases"
    (je/with-db-env [env *cupboard-path*]
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
          (je/with-db [shelves-db env *shelves-db-name*]
            (je/with-db-cursor [cur1 shelves-db]
              (is (= (je/db-cursor-first cur1) [*default-shelf-name* {}]))
              (is (= (je/db-cursor-next cur1)
                     [idx-name-age {:sorted-duplicates true}]))
              (is (= (je/db-cursor-next cur1)
                     [idx-name-bank-acct {:sorted-duplicates false}]))
              (is (= (je/db-cursor-next cur1)
                     [idx-name-first-name {:sorted-duplicates true}]))
              (is (= (je/db-cursor-next cur1)
                     [idx-name-last-name {:sorted-duplicates true}]))
              (is (= (je/db-cursor-next cur1)
                     [idx-name-login {:sorted-duplicates false}]))
              (is (= (je/db-cursor-next cur1) [])))))))))


(deftest basics
  (let [cupboard-location (make-temp-dir)
        p1 (atom nil)
        p2 (atom nil)
        p3 (atom nil)
        p4 (atom nil)]

    (testing "default *cupboard*"
      (try
       (with-open-cupboard [cupboard-location]
         (reset! p1 (make-instance president ["gw" "George" "Washington" 57]))
         (reset! p2 (make-instance president ["ja" "John" "Adams" 62]))
         (reset! p3 (make-instance president ["tj" "Thomas" "Jefferson" 58]))
         (reset! p4 (make-instance president ["jm" "James" "Madison" 58])))

       (testing "ability to build a struct-map on retrieve"
         (with-open-cupboard [cupboard-location]
           (is (= @p1 (retrieve :login "gw")))
           (let [sp1 (retrieve :login "gw" :struct president)]
             (is (= (type sp1) clojure.lang.PersistentStructMap))
             (is (= (meta sp1) (meta @p1))))))

       (testing "plain hash-map retrieval"
         (with-open-cupboard [cupboard-location]
           (is (= @p1 (retrieve :login "gw")))
           (is (= @p2 (retrieve :login "ja" :cupboard *cupboard*)))
           (is (= @p3 (retrieve :login "tj")))
           (is (= @p4 (retrieve :login "jm")))
           (testing ":any index retrieval"
             (is (= (set (retrieve :age 58)) #{@p4 @p3})))
           (testing "deletion"
             (delete @p2)
             (is (nil? (retrieve :login "ja"))))))
         (finally
          (rmdir-recursive cupboard-location))))

    (testing "explicitly bound cupboard"
      (try
       (with-open-cupboard [cb cupboard-location]
         (reset! p1 (make-instance president ["gw" "George" "Washington" 57] :cupboard cb))
         (reset! p2 (make-instance president ["ja" "John" "Adams" 62] :cupboard cb))
         (reset! p3 (make-instance president ["tj" "Thomas" "Jefferson" 58] :cupboard cb))
         (reset! p4 (make-instance president ["jm" "James" "Madison" 58] :cupboard cb)))
       (with-open-cupboard [cb cupboard-location]
         (is (= @p1 (retrieve :login "gw" :cupboard cb)))
         (is (= @p2 (retrieve :login "ja" :cupboard cb)))
         (is (= @p3 (retrieve :login "tj" :cupboard cb)))
         (is (= @p4 (retrieve :login "jm" :cupboard cb)))
         (is (thrown? NullPointerException (retrieve :login "jm"))))
       (finally
        (rmdir-recursive cupboard-location))))))


(deftest transaction-basics
  (with-open-cupboard [*cupboard-path*]
    (let [p1 {:login "gw" :first-name "George" :last-name "Washington" :age 57 :bank-acct nil}]

      (testing "basic transactions"
        (with-txn []
          (make-instance president ["gw" "George" "Washington" 57])
          (is (= (retrieve :login "gw") p1))
          (rollback)
          (is (thrown-with-msg?
                RuntimeException #".*non-open transaction"
                (make-instance president ["ja" "John" "Adams" 62]))))
        (is (empty? (retrieve :login "gw")))
        (is (empty? (retrieve :login "ja")))
        (with-txn [:write-no-sync true]
          (make-instance president ["gw" "George" "Washington" 57])
          (commit)
          (is (thrown-with-msg?
                RuntimeException #".*non-open transaction"
                (make-instance president ["ja" "John" "Adams" 62]))))
        (is (empty? (retrieve :login "ja")))
        (is (= (retrieve :login "gw") p1)))

      (testing "transactional shelf removal"
        (make-instance president ["aj" "Andrew" "Johnson"] :shelf-name "presidents")
        (with-txn []
          (is (= (retrieve :login "aj" :shelf-name "presidents")
                 {:login "aj" :first-name "Andrew" :last-name "Johnson"
                  :bank-acct nil :age nil}))
          (remove-shelf "presidents")
          (is (not (some #(= % "presidents") (list-shelves))))
          (rollback))
        (is (some #(= % "presidents") (list-shelves)))))))


(deftest transaction-binding
  (with-open-cupboard [*cupboard-path*]
    (testing "lexically bound transaction"
      (with-txn [txn1]
        (make-instance president ["gw" "George" "Washington" 57] :txn txn1)
        (rollback txn1))
      (is (empty? (retrieve :login "gw"))))))


(deftest deadlocks
  ;; Cannot use with-open-cupboard because Clojure's dynamic variables do not
  ;; propagate to child threads.
  (let [cb (open-cupboard *cupboard-path*)]
    (try
     (let [gw (make-instance president ["gw" "George" "Washington" 57] :cupboard cb)
           ja (make-instance president ["ja" "John" "Adams" 62] :cupboard cb)
           done-1 (atom false)
           done-2 (atom false)]

       (testing "deadlock resolution, both threads commit"
         (.start (Thread. (fn []
                            (with-txn [:cupboard cb :max-attempts 2 :retry-delay-msec 10]
                              (passoc! gw :bank-acct 1 :cupboard cb)
                              (Thread/sleep 50)
                              (passoc! ja :bank-acct 2 :cupboard cb))
                            (reset! done-1 true))))
         (.start (Thread. (fn []
                            (with-txn [:cupboard cb :max-attempts 2 :retry-delay-msec 250]
                              (passoc! ja :bank-acct 3 :cupboard cb)
                              (Thread/sleep 50)
                              (passoc! gw :bank-acct 4 :cupboard cb))
                            (reset! done-2 true))))
         ;; wait for threads to complete
         (loop [i 0]
           (when-not (and @done-1 @done-2)
             (Thread/sleep 100)
             (recur (inc i))))
         ;; The first thread has a shorter retry delay, so it should win the
         ;; race. It commits first, then the second thread overwrites the
         ;; values.
         (is (= (retrieve :login "gw" :cupboard cb) (assoc gw :bank-acct 4)))
         (is (= (retrieve :login "ja" :cupboard cb) (assoc ja :bank-acct 3))))

       (testing "deadlock resolution, one thread rolls back permanently"
         (reset! done-1 false)
         (reset! done-2 false)
         (.start (Thread. (fn []
                            (with-txn [:cupboard cb :max-attempts 2 :retry-delay-msec 100]
                              (passoc! gw :bank-acct 5 :cupboard cb)
                              (Thread/sleep 100)
                              (passoc! ja :bank-acct 6 :cupboard cb))
                            (reset! done-1 true))))
         (.start (Thread. (fn []
                            (is (thrown? RuntimeException
                                  (try
                                   (with-txn [:cupboard cb :max-attempts 1]
                                     (passoc! ja :bank-acct 7 :cupboard cb)
                                     (Thread/sleep 10)
                                     (passoc! gw :bank-acct 8 :cupboard cb))
                                   (finally
                                    (reset! done-2 true))))))))
         ;; wait for threads to complete
         (loop [i 0]
           (when-not (and @done-1 @done-2)
             (Thread/sleep 100)
             (recur (inc i))))
         ;; Only the first thread should commit here.
         (is (= (retrieve :login "gw" :cupboard cb) (assoc gw :bank-acct 5)))
         (is (= (retrieve :login "ja" :cupboard cb) (assoc ja :bank-acct 6)))))

     (finally
      (close-cupboard cb)))))


(deftest simple-concurrency
  (with-open-cupboard [c *cupboard-path*]
    (let [a1 (agent nil)
          a2 (agent nil)]
      (send a1 (fn [_]
                 (make-instance president ["gw" "George" "Washington" 57] :cupboard c)
                 (make-instance president ["ja" "John" "Adams" 62] :cupboard c)))
      (send a2 (fn [_]
                 (make-instance president ["tj" "Thomas" "Jefferson" 58] :cupboard c)
                 (make-instance president ["jm" "James" "Madison" 58] :cupboard c)))
      (await a1 a2)
      (is (= (shelf-count :cupboard c) 4)))))


(deftest passoc!-pdissoc!
  (with-open-cupboard [*cupboard-path*]
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
        (let [p (atom (make-instance president ["gw" "George" "Washington" 57]))]
          (is (= (retrieve :login "gw") gw1))
          (reset! p (passoc! @p :bank-acct 1))
          (is (= (retrieve :login "gw") gw2))
          (reset! p (passoc! @p :birthday date-gw))
          (is (= (retrieve :login "gw") gw3))
          (reset! p (pdissoc! @p :birthday))
          (is (= (retrieve :login "gw") gw2))))

      (testing "passoc!-pdissoc! operations on non-default shelves"
        (let [p (atom (make-instance president ["ja" "John" "Adams" 62]
                                        :shelf-name "presidents"))]
          (is (= (retrieve :login "ja" :shelf-name "presidents") ja1))
          (reset! p (passoc! @p :bank-acct 2))
          (is (= (retrieve :login "ja" :shelf-name "presidents") ja2))
          (reset! p (passoc! @p :birthday date-ja))
          (is (= (retrieve :login "ja" :shelf-name "presidents") ja3))
          (reset! p (pdissoc! @p :birthday))
          (is (= (retrieve :login "ja" :shelf-name "presidents") ja2))))

      (testing "passoc!-pdissoc! operations with multiple operands"
        (let [p (atom (make-instance president ["tj" "Thomas" "Jefferson" 58]))]
          (is (= (retrieve :login "tj") tj1))
          (reset! p (passoc! @p [:bank-acct 3 :birthday date-tj]))
          (is (= (retrieve :login "tj") tj3))
          (reset! p (passoc! @p [:nonce1 1 :nonce2 2]))
          (reset! p (pdissoc! @p [:birthday :nonce1 :nonce2]))
          (is (= (retrieve :login "tj") tj2)))))))


(deftest queries
  (with-open-cupboard [*cupboard-path*]
    (let [p1 (make-instance president ["gw" "George" "Washington" 57])
          p2 (make-instance president ["ja" "John" "Adams" 62])
          p3 (make-instance president ["tj" "Thomas" "Jefferson" 58])
          p4 (make-instance president ["jm1" "James" "Madison" 58])
          p5 (make-instance president ["jm2" "James" "Monroe" 59])
          p6 (make-instance president ["jqa" "John" "Adams" 58])
          p7 (make-instance president ["aj" "Andrew" "Jackson" 62])
          p8 (make-instance president ["mvb" "Martin" "Van Buren" 55])
          p9 (make-instance president ["whh" "William" "Harrison" 68])
          p10 (make-instance president ["jt" "John" "Tyler" 51])]

      (testing "no-clause query (list full database contents)"
        (is (= (set (query))
               #{p1 p2 p3 p4 p5 p6 p7 p8 p9 p10})))

      (testing "basic one-clause query operations"
        (is (= (set (query (= :login "gw"))) #{p1}))
        (is (= (set (query (= :login "aj"))) #{p7}))
        (is (= (set (query (= :age 57))) #{p1}))
        (is (empty? (query (= :age 57) (= :age 62))))
        (is (= (set (query (= :age 62))) #{p2 p7}))
        (is (= (set (query (<= :age 55))) #{p8 p10}))
        (is (= (set (query (< :age 55))) #{p10}))
        (is (= (set (query (> :age 60))) #{p2 p7 p9})))

      (testing "queries with multiple clauses"
        (is (= (set (query (< :age 60) (starts-with :first-name "J"))) #{p4 p5 p6 p10}))
        (is (= (set (query (< :age 60) (starts-with :first-name "Ja"))) #{p4 p5}))
        (is (= (set (query (< :age 60) (= :first-name "John"))) #{p6 p10}))
        (is (= (set (query (= :first-name "John"))) #{p2 p6 p10}))
        (is (= (count (query (< :age 60) (starts-with :first-name "J") :limit 2)) 2)))

      (testing "destructive callbacks"
        (query (< :age 60) (= :first-name "John")
                  :callback #(passoc! % :first-name "Jack"))
        (is (= (retrieve :login "ja") p2))
        (is (= (retrieve :login "jqa") (assoc p6 :first-name "Jack"))))

      (testing "making sure natural joins are used wherever possible"
        (let [q (macroexpand-1
                 '(cupboard.core/query (= :age 58) (= :last-name "Adams")
                                       :callback #(passoc! % :first-name "John Quincy")))]
          (is (= (first (first (rest (rest (rest (first (rest q)))))))
                 'cupboard.core/query-natural-join)))
        (query (= :age 58) (= :last-name "Adams")
                  :callback #(passoc! % :first-name "John Quincy"))
        (is (= (retrieve :login "jqa") (assoc p6 :first-name "John Quincy"))))

      (testing "delete as a callback"
        (query (= :age 58) :callback delete)
        (is (nil? (retrieve :login "tj")))
        (is (nil? (retrieve :login "jm1")))
        (is (nil? (retrieve :login "jqa"))))

      (testing "making sure that :struct applied to query works"
        (let [everyone (query (> :age 50) :struct president)]
          (is (> (count everyone) 0))
          (is (every? #(= (type %) clojure.lang.PersistentStructMap) everyone)))))))
