(ns test.cupboard
  (:use [clojure.contrib test-is])
  (:use [cupboard utils])
  (:use [cupboard :as cb]))


(cb/defpersist president
  ((:login :index :unique)
   (:first-name :index :any)
   (:last-name :index :any)
   (:age :index :any)
   (:bank-acct :index :unique))
  :primary-key :login
  :shelf "presidents")


(cb/defpersist president-defaults
  ((:login :index :unique)
   (:first-name :index :any)
   (:last-name :index :any)
   (:age :index :any)
   (:bank-acct :index :unique)
   (:other)))


(cb/defpersist president-bad-struct
  ((:login)
   (:first-name)
   (:last-name)
   (:age)
   (:bank-acct)
   (:other)))


(deftest persistent-structures-1
  (let [p1 (cb/make-instance president :save false "gw" "George" "Washington" 57)
        p2 (cb/make-instance president :save false "ja" "John" "Adams" 62)
        p3 (cb/make-instance president :save false "tj" "Thomas" "Jefferson" 58)
        p4 (cb/make-instance president :save false "jm" "James" "Madison" 58)]
    (is (= (p1 :login) "gw"))
    (is (= (p2 :first-name) "John"))
    (is (= (p3 :age) 58))
    (is (nil? (p4 :bank-acct)))
    (is (= ((meta p1) :primary-key) :login))
    (is (= ((meta p2) :shelf) "presidents"))
    (is (= ((meta p2) :index-uniques) [:bank-acct]))
    (is (= ((meta p2) :index-anys) [:first-name :last-name :age]))))


(deftest persistent-structures-2
  (let [p1 (cb/make-instance president-defaults :save false "gw" "George" "Washington" 57)
        p2 (cb/make-instance president-defaults :save false "ja" "John" "Adams" 62)
        p3 (cb/make-instance president-defaults :save false "tj" "Thomas" "Jefferson" 58)
        p4 (cb/make-instance president-defaults :save false "jm" "James" "Madison" 58)]
    (is (= (p1 :login) "gw"))
    (is (= (p2 :first-name) "John"))
    (is (= (p3 :age) 58))
    (is (nil? (p4 :bank-acct)))
    (is (= ((meta p1) :primary-key) :login))
    (is (= ((meta p2) :index-uniques) [:bank-acct]))
    (is (= ((meta p2) :index-anys) [:first-name :last-name :age]))))


(deftest persistent-structures-3
  (let [p1 (cb/make-instance president-bad-struct :save false "gw" "George" "Washington" 57)
        p2 (cb/make-instance president-bad-struct :save false "ja" "John" "Adams" 62)
        p3 (cb/make-instance president-bad-struct :save false "tj" "Thomas" "Jefferson" 58)
        p4 (cb/make-instance president-bad-struct :save false "jm" "James" "Madison" 58)]
    (is (= (p1 :login) "gw"))
    (is (= (p2 :first-name) "John"))
    (is (= (p3 :age) 58))
    (is (nil? (p4 :bank-acct)))
    (is (nil? ((meta p1) :primary-key)))
    (is (= ((meta p2) :index-uniques) []))
    (is (= ((meta p2) :index-anys) []))))


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
       (is (= @p4 (cb/retrieve :login "jm"))))
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


;;; TODO: Write a test for shelf functionality.


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
