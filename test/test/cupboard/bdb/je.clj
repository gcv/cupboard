(ns test.cupboard.bdb.je
  (:use cupboard.utils cupboard.bdb.je)
  (:use [clojure.contrib test-is])
  (:import [com.sleepycat.je OperationStatus DatabaseException]))



;;; ----------------------------------------------------------------------------
;;; fixtures
;;; ----------------------------------------------------------------------------

(declare *db-path* *db-env* *db*)


(defn fixture-db-path [f]
  (binding [*db-path* (.getAbsolutePath (make-temp-dir))]
    (f)
    (rmdir-recursive *db-path*)))


(defn fixture-db-env [f]
  (binding [*db-env* (db-env-open *db-path* :allow-create true)]
    (f)
    (db-env-close *db-env*)))


(defn fixture-db [f]
  (binding [*db* (db-open *db-env* "db" :allow-create true)]
    (f)
    (db-close *db*)))


(use-fixtures :each fixture-db-path fixture-db-env fixture-db)



;;; ----------------------------------------------------------------------------
;;; tests
;;; ----------------------------------------------------------------------------

(deftest basic-db-open-close
  (let [path (make-temp-dir)]
    (try
     (let [e (db-env-open path :allow-create true :transactional true)
           db (db-open e "db" :allow-create true)
           idx1 (db-sec-open e db "idx1" :allow-create true :key-creator-fn :idx1)
           idx2 (db-sec-open e db "idx2" :allow-create true :key-creator-fn :idx2)
           txn1 (db-txn-begin e)
           txn2 (db-txn-begin e)
           cur1 (db-cursor-open idx1)
           cur2 (db-cursor-open idx2)
           jcur1 (db-join-cursor-open [cur1 cur2])]
       (is (not (nil? @(jcur1 :join-cursor-handle))))
       (db-join-cursor-close jcur1)
       (is (nil? @(jcur1 :join-cursor-handle)))
       (is (not (nil? @(cur2 :cursor-handle))))
       (db-cursor-close cur2)
       (is (nil? @(cur2 :cursor-handle)))
       (is (not (nil? @(cur1 :cursor-handle))))
       (db-cursor-close cur1)
       (is (nil? @(cur1 :cursor-handle)))
       (is (not (nil? @(txn2 :txn-handle))))
       (db-txn-commit txn2)
       (is (nil? @(txn2 :txn-handle)))
       (is (not (nil? @(txn1 :txn-handle))))
       (db-txn-abort txn1)
       (is (nil? @(txn1 :txn-handle)))
       (is (not (nil? @(idx2 :db-sec-handle))))
       (db-sec-close idx2)
       (is (nil? @(idx2 :db-sec-handle)))
       (is (not (nil? @(idx1 :db-sec-handle))))
       (db-sec-close idx1)
       (is (nil? @(idx1 :db-sec-handle)))
       (is (not (nil? @(db :db-handle))))
       (db-close db)
       (is (nil? @(db :db-handle)))
       (is (not (nil? @(e :env-handle))))
       (db-env-close e)
       (is (nil? @(e :env-handle))))
     (finally
      (rmdir-recursive path)))))


(deftest db-operations
  (is (thrown? DatabaseException (db-open *db-env* "not here" :allow-create false)))
  (with-db [db *db-env* "newly created" :allow-create true])
  (with-db [db *db-env* "newly created" :allow-create false]
    (is (zero? (db-count db))))
  (db-env-rename-db *db-env* "newly created" "my-db")
  (with-db [db *db-env* "my-db" :allow-create false]
    (is (zero? (db-count db)))
    (db-put db "one" 1)
    (db-put db "two" 2)
    (is (= (db-count db) 2)))
  (is (= (db-env-truncate-db *db-env* "my-db" :count true) 2))
  (db-env-remove-db *db-env* "my-db")
  (is (thrown? DatabaseException (db-open *db-env* "my-db" :allow-create false)))
  ;; test statistics (doesn't check for validity, just makes sure the code runs)
  (let [stats (db-env-stats *db-env*
                            :n-cache-bytes true
                            :n-cache-misses true
                            :n-fsyncs true
                            :n-random-reads true
                            :n-random-writes true
                            :n-seq-reads true
                            :n-seq-writes true
                            :n-total-log-bytes true
                            :n-txn-active true
                            :n-txn-begins true
                            :n-txn-aborts true
                            :n-txn-commits true
                            :n-lock-owners true
                            :n-read-locks true
                            :n-total-locks true
                            :n-lock-waiting-txns true
                            :n-write-locks true
                            :n-lock-requests true
                            :n-lock-waits true)]
    (is (every? identity (map #(contains? stats %)
                              [:n-cache-bytes :n-cache-misses :n-fsyncs
                               :n-random-reads :n-random-writes :n-seq-reads
                               :n-seq-writes :n-total-log-bytes :n-txn-active
                               :n-txn-begins :n-txn-aborts :n-txn-commits
                               :n-lock-owners :n-read-locks :n-total-locks
                               :n-lock-waiting-txns :n-write-locks :n-lock-requests
                               :n-lock-waits])))))


(deftest basics
  (db-put *db* "one" 1)
  (is (= (db-get *db* "one") ["one" 1]))
  (dotimes [i 100] (db-put *db* (str i) i))
  (is (= (db-get *db* "50") ["50" 50]))
  (is (empty? (db-get *db* "not there")))
  (db-delete *db* "one")
  (is (empty? (db-get *db* "one"))))


(deftest types
  (let [now (java.util.Date.)
        uuid (java.util.UUID/randomUUID)]
    (db-put *db* "nil" nil)
    (db-put *db* "boolean" true)
    (db-put *db* "char" \c)
    (db-put *db* "byte" (byte 1))
    (db-put *db* "short" (short 1))
    (db-put *db* "int" (int 1))
    (db-put *db* "long" (long 1))
    (db-put *db* "bigint" (bigint 1))
    (db-put *db* "ratio" (/ 1 2))
    (db-put *db* "double" 1.0)
    (db-put *db* "string" "hello world")
    (db-put *db* "date" now)
    (db-put *db* "uuid" uuid)
    (db-put *db* "keyword" :one)
    (db-put *db* "symbol" 'one)
    (db-put *db* "list" (list 1 2 3))
    (db-put *db* "vector" [1 2 3])
    (db-put *db* "map" {:one 1 :two 2 :three 3})
    (db-put *db* "set" #{:one 2 'three})
    (is (= (db-get *db* "nil") ["nil" nil]))
    (is (= (db-get *db* "boolean") ["boolean" true]))
    (is (= (db-get *db* "char") ["char" \c]))
    (is (= (db-get *db* "byte") ["byte" (byte 1)]))
    (is (= (db-get *db* "short") ["short" (short 1)]))
    (is (= (db-get *db* "int") ["int" (int 1)]))
    (is (= (db-get *db* "long") ["long" (long 1)]))
    (is (= (db-get *db* "bigint") ["bigint" (bigint 1)]))
    (is (= (db-get *db* "ratio") ["ratio" (/ 1 2)]))
    (is (= (db-get *db* "double") ["double" 1.0]))
    (is (= (db-get *db* "string") ["string" "hello world"]))
    (is (= (db-get *db* "date") ["date" now]))
    (is (= (db-get *db* "uuid") ["uuid" uuid]))
    (is (= (db-get *db* "keyword") ["keyword" :one]))
    (is (= (db-get *db* "symbol") ["symbol" 'one]))
    (is (= (db-get *db* "list") ["list" (list 1 2 3)]))
    (is (= (db-get *db* "vector") ["vector" [1 2 3]]))
    (is (= (db-get *db* "map") ["map" {:one 1 :two 2 :three 3}]))
    (is (= (db-get *db* "set") ["set" #{:one 2 'three}]))))


(deftest duplicates
  (is (= (db-put *db* "a" 1) OperationStatus/SUCCESS))
  (is (= (db-put *db* "a" 1 :no-overwrite true) OperationStatus/KEYEXIST))
  (with-db [db *db-env* "db2" :allow-create true :sorted-duplicates true]
    (is (= (db-put db "a" 1) OperationStatus/SUCCESS))
    (is (= (db-put db "b" 2) OperationStatus/SUCCESS))
    (is (= (db-put db "a" 3) OperationStatus/SUCCESS))
    (is (= (db-put db "b" 2 :no-dup-data true) OperationStatus/KEYEXIST))
    (is (= (db-put db "b" 4 :no-overwrite true) OperationStatus/KEYEXIST))
    (with-db-cursor [cur1 db]
      (is (= (db-cursor-search cur1 "a" :data 3 :search-both true) ["a" 3]))
      (is (= (db-cursor-search cur1 "a") ["a" 1]))))
  (db-env-remove-db *db-env* "db2"))


(deftest cursors
  (let [data1 #{:one 1 :two 2 :three "three" :four "five six seven eight"}
        data2 "hello world"
        data3 3.3
        data4 [4]
        data5 {:one 1 :two 2}]
    (db-put *db* "a" data1)
    (db-put *db* "b" data2)
    (db-put *db* "c" data3)
    (db-put *db* "d" data4)
    (with-db-cursor [cur1 *db*]
      (is (= (db-cursor-search cur1 "a") ["a" data1]))
      (is (= (db-cursor-next cur1) ["b" data2]))
      (is (= (db-cursor-next cur1) ["c" data3]))
      (is (= (db-cursor-next cur1 :direction :back) ["b" data2]))
      (db-cursor-put cur1 "e" data5)
      (is (= (db-cursor-next cur1 :direction :back) ["d" data4]))
      (is (= (db-cursor-next cur1 :direction :forward) ["e" data5]))
      (db-cursor-delete cur1)
      (is (= (db-cursor-next cur1 :direction :back) ["d" data4]))
      (is (empty? (db-cursor-next cur1 :direction :forward)))
      (db-cursor-put cur1 "e" data5)
      (db-cursor-replace cur1 data1)
      (db-cursor-next cur1 :direction :back)
      (is (= (db-cursor-next cur1 :direction :forward) ["e" data1]))
      (is (= (db-cursor-current cur1) ["e" data1]))
      (is (= (db-cursor-first cur1) ["a" data1]))
      (is (= (db-cursor-last cur1) ["e" data1]))
      (is (= (db-cursor-search cur1 "a" :data data1 :search-both true) ["a" data1])))))


(deftest indices
  (let [data1 {:id 1 :first-name "Aardvark" :last-name "Aardvarkov"}
        data2 {:id 2 :first-name "Baran" :last-name "Baranovich"}
        data3 {:id 3 :first-name "Beleg" :last-name "Cuthalion"}]
    (with-db-sec [idx1 *db-env* *db* "idx1" :allow-create true :key-creator-fn :last-name]
      (db-put *db* 1 data1)
      (db-put *db* 2 data2)
      (db-put *db* 3 data3)
      (is (= (db-get *db* 1) [1 data1]))
      (is (= (db-sec-get idx1 "Cuthalion") [3 data3]))
      (is (empty? (db-sec-get idx1 "Balkonsky")))
      (db-sec-delete idx1 "Baranovich")
      (is (empty? (db-get *db* 2)))
      (is (empty? (db-sec-get idx1 "Baranovich"))))))


(deftest index-cursors
  (let [data1 {:id 1 :first-name "Aardvark" :last-name "Aardvarkov"}
        data2 {:id 2 :first-name "Baran" :last-name "Baranovich"}
        data3 {:id 3 :first-name "Beleg" :last-name "Cuthalion"}]
    (with-db-sec [idx1 *db-env* *db* "idx1" :allow-create true :key-creator-fn :last-name]
      (db-put *db* 1 data1)
      (db-put *db* 2 data2)
      (db-put *db* 3 data3)
      (with-db-cursor [cur1 idx1]
        (is (= (db-cursor-search cur1 "Baran") [2 data2]))
        (is (= (db-cursor-next cur1) [3 data3]))))))


(deftest joins
  (let [car-1 {:marque "BMW" :model "X3" :year 2006 :color "blue" :awd true}
        car-2 {:marque "Audi" :model "TT" :year 2002 :color "blue" :awd true}
        car-3 {:marque "Audi" :model "allroad" :year 2002 :color "grey" :awd true}
        car-4 {:marque "Audi" :model "A6 Avant" :year 2006 :color "white" :awd true}
        car-5 {:marque "Audi" :model "Q5" :year 2010 :color "white" :awd true}
        car-6 {:marque "Ford" :model "Taurus" :year 1996 :color "beige" :awd false}
        car-7 {:marque "BMW" :model "330i" :year 2003 :color "blue" :awd false}
        car-8 {:marque "Eagle" :model "Summit" :year 1994 :color "blue" :awd false}
        car-9 {:marque "Audi" :model "A4" :year 2003 :color "blue" :awd false}]
    (with-db-sec [idx-color *db-env* *db* "idx-color"
                  :allow-create true :sorted-duplicates true
                  :key-creator-fn :color]
      (with-db-sec [idx-year *db-env* *db* "idx-year"
                    :allow-create true :sorted-duplicates true
                    :key-creator-fn :year]
        (with-db-sec [idx-awd *db-env* *db* "idx-awd"
                      :allow-create true :sorted-duplicates true
                      :key-creator-fn :awd]
          (db-put *db* 1 car-1)
          (db-put *db* 2 car-2)
          (db-put *db* 3 car-3)
          (db-put *db* 4 car-4)
          (db-put *db* 5 car-5)
          (db-put *db* 6 car-6)
          (db-put *db* 7 car-7)
          (db-put *db* 8 car-8)
          (db-put *db* 9 car-9)
          (with-db-cursor [c1 idx-year]
            (with-db-cursor [c2 idx-color]
              (is (= (db-cursor-search c1 2003) [7 car-7]))
              (is (= (db-cursor-search c2 "blue") [1 car-1]))
              (with-db-join-cursor [j [c1 c2]]
                (is (= (db-join-cursor-next j) [7 car-7]))
                (is (= (db-join-cursor-next j) [9 car-9]))
                (is (empty? (db-join-cursor-next j)))))))))))


(deftest cursor-scans-1
  (with-db-sec [idx1 *db-env* *db* "idx1"
                :allow-create true
                :key-creator-fn :a
                :sorted-duplicates true]
    ;; (1 2 3 3 3 5 5 7 nil 11 12)
    (db-put *db* 1 {:a 1})
    (db-put *db* 2 {:a 2})
    (db-put *db* 3 {:a 3})
    (db-put *db* 4 {:a 3})
    (db-put *db* 5 {:a 3})
    (db-put *db* 6 {:a 5})
    (db-put *db* 7 {:a 5})
    (db-put *db* 8 {:a 7})
    (db-put *db* 11 {:a 11})
    (db-put *db* 12 {:a 12})
    (with-db-cursor [cur1 idx1]
      (is (= (db-cursor-scan cur1 1)
             [ [1 {:a 1}] ]))
      (is (= (db-cursor-scan cur1 3)
             [ [3 {:a 3}] [4 {:a 3}] [5 {:a 3}] ]))
      (is (empty? (db-cursor-scan cur1 6)))
      (is (= (db-cursor-scan cur1 4 :comparison-fn <)
             [ [5 {:a 3}] [4 {:a 3}] [3 {:a 3}] [2 {:a 2}] [1 {:a 1}] ]))
      (is (= (db-cursor-scan cur1 5 :comparison-fn <=)
             [ [6 {:a 5}] [5 {:a 3}] [4 {:a 3}] [3 {:a 3}] [2 {:a 2}] [1 {:a 1}] ]))
      (is (= (db-cursor-scan cur1 4 :comparison-fn >)
             [ [6 {:a 5}] [7 {:a 5}] [8 {:a 7}] [11 {:a 11}] [12 {:a 12}] ]))
      (is (= (db-cursor-scan cur1 2 :comparison-fn >)
             [ [3 {:a 3}] [4 {:a 3}] [5 {:a 3}] [6 {:a 5}] [7 {:a 5}]
               [8 {:a 7}] [11 {:a 11}] [12 {:a 12}] ]))
      (is (= (take 2 (db-cursor-scan cur1 2 :comparison-fn >=))
             [ [2 {:a 2}] [3 {:a 3}] ])))
    (with-db-cursor [cur2 *db*]
      (is (= (db-cursor-scan cur2 4) [ [4 {:a 3}] ]))
      (is (empty? (db-cursor-scan cur2 10)))
      (is (= (db-cursor-scan cur2 10 :comparison-fn < :direction :back)
             [ [8 {:a 7}] [7 {:a 5}] [6 {:a 5}] [5 {:a 3}] [4 {:a 3}]
               [3 {:a 3}] [2 {:a 2}] [1 {:a 1}] ]))
      (is (= (db-cursor-scan cur2 10 :comparison-fn >)
             [ [11 {:a 11}] [12 {:a 12}] ]))
      (is (= (db-cursor-scan cur2 11 :comparison-fn >=)
             [ [11 {:a 11}] [12 {:a 12}] ]))
      (is (= (db-cursor-scan cur2 11 :comparison-fn >)
             [ [12 {:a 12}] ])))))


(deftest cursor-scans-2
  (with-db-sec [idx1 *db-env* *db* "idx1"
                :allow-create true
                :key-creator-fn :a
                :sorted-duplicates true]
    (db-put *db* 1 {:a "abc123"})
    (db-put *db* 2 {:a "def456"})
    (db-put *db* 3 {:a "def789"})
    (db-put *db* 4 {:a "ghi789"})
    (with-db-cursor [cur1 idx1]
      (is (= (db-cursor-scan cur1 "def" :comparison-fn starts-with)
             [ [2 {:a "def456"}] [3 {:a "def789"}] ])))))


(deftest transactions
  (let [path (make-temp-dir)]
    (with-db-env [e path :allow-create true :transactional true]
      (with-db [db e "db" :allow-create true]
        (with-db-sec [idx1 e db "idx1" :key-creator-fn :login :allow-create true]
          ;; basic transactionality tests
          (db-put db "one" 1)
          (with-db-txn [txn1 e]
            (db-put db "two" 2 :txn txn1)
            (db-put db "three" 3 :txn txn1))
          (is (= (db-get db "one") ["one" 1]))
          (is (= (db-get db "two") ["two" 2]))
          (is (= (db-get db "three") ["three" 3]))
          (with-db-txn [txn2 e]
            (db-put db "four" 4 :txn txn2)
            (db-put db "five" 5 :txn txn2)
            (db-txn-abort txn2))
          (is (empty? (db-get db "four")))
          (is (empty? (db-get db "five")))
          (with-db-txn [txn3 e]
            (db-put db "six" 6)
            (db-put db "seven" 7 :txn txn3)
            (db-txn-abort txn3))
          (is (= (db-get db "six") ["six" 6]))
          (is (empty? (db-get db "seven")))
          ;; tests for secondary database transactionality
          (db-put db 1 {:login "gw" :name "George Washington"})
          (db-put db 2 {:login "ja" :name "John Adams"})
          (db-put db 3 {:login "tj" :name "Thomas Jefferson"})
          (db-put db 4 {:login "jm" :name "James Madison"})
          (with-db-txn [txn4 e]
            (with-db-cursor [cur1 idx1 :txn txn4]
              (db-cursor-search cur1 "j")
              (db-cursor-delete cur1)
              (db-cursor-next cur1)
              (db-cursor-delete cur1))
            (db-txn-abort txn4))
          (is (= (db-get db 2) [2 {:login "ja" :name "John Adams"}]))
          (is (= (db-get db 4) [4 {:login "jm" :name "James Madison"}]))
          (with-db-txn [txn5 e]
            (with-db-cursor [cur2 idx1 :txn txn5]
              (db-cursor-search cur2 "j")
              (db-cursor-delete cur2)
              (db-cursor-next cur2)
              (db-cursor-delete cur2)))
          (is (empty? (db-get db 2)))
          (is (empty? (db-get db 4))))))
    (rmdir-recursive path)))
