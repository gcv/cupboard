(ns test.cupboard.db-core
  (:use [cupboard utils db-core])
  (:use [clojure.contrib test-is]))



;;; ----------------------------------------------------------------------
;;; fixtures
;;; ----------------------------------------------------------------------

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



;;; ----------------------------------------------------------------------
;;; tests
;;; ----------------------------------------------------------------------

(deftest basics
  (db-put *db* "one" 1)
  (is (= (db-get *db* "one") ["one" 1]))
  (dotimes [i 100] (db-put *db* (str i) i))
  (is (= (db-get *db* "50") ["50" 50]))
  (is (= (db-get *db* "not there") []))
  (db-delete *db* "one")
  (is (= (db-get *db* "one") [])))


(deftest types
  (let [now (java.util.Date.)]
    (db-put *db* "nil"     nil)
    (db-put *db* "boolean" true)
    (db-put *db* "char"    \c)
    (db-put *db* "byte"    (byte 1))
    (db-put *db* "short"   (short 1))
    (db-put *db* "int"     (int 1))
    (db-put *db* "long"    (long 1))
    (db-put *db* "bigint"  (bigint 1))
    (db-put *db* "ratio"   (/ 1 2))
    (db-put *db* "double"  1.0)
    (db-put *db* "string"  "hello world")
    (db-put *db* "date"    now)
    (db-put *db* "keyword" :one)
    (db-put *db* "symbol"  'one)
    (db-put *db* "list"    (list 1 2 3))
    (db-put *db* "vector"  [1 2 3])
    (db-put *db* "map"     {:one 1 :two 2 :three 3})
    (db-put *db* "set"     #{:one 2 'three})
    (is (= (db-get *db* "nil")     ["nil" nil]))
    (is (= (db-get *db* "boolean") ["boolean" true]))
    (is (= (db-get *db* "char")    ["char" \c]))
    (is (= (db-get *db* "byte")    ["byte" (byte 1)]))
    (is (= (db-get *db* "short")   ["short" (short 1)]))
    (is (= (db-get *db* "int")     ["int" (int 1)]))
    (is (= (db-get *db* "long")    ["long" (long 1)]))
    (is (= (db-get *db* "bigint")  ["bigint" (bigint 1)]))
    (is (= (db-get *db* "ratio")   ["ratio" (/ 1 2)]))
    (is (= (db-get *db* "double")  ["double" 1.0]))
    (is (= (db-get *db* "string")  ["string" "hello world"]))
    (is (= (db-get *db* "date")    ["date" now]))
    (is (= (db-get *db* "keyword") ["keyword" :one]))
    (is (= (db-get *db* "symbol")  ["symbol" 'one]))
    (is (= (db-get *db* "list")    ["list" (list 1 2 3)]))
    (is (= (db-get *db* "vector")  ["vector" [1 2 3]]))
    (is (= (db-get *db* "map")     ["map" {:one 1 :two 2 :three 3}]))
    (is (= (db-get *db* "set")     ["set" #{:one 2 'three}]))))


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
    (with-db-cursor cur1 [*db*]
      (is (= (db-cursor-search cur1 "a") ["a" data1]))
      (is (= (db-cursor-next cur1) ["b" data2]))
      (is (= (db-cursor-next cur1) ["c" data3]))
      (is (= (db-cursor-next cur1 :direction :back) ["b" data2]))
      (db-cursor-put cur1 "e" data5)
      (is (= (db-cursor-next cur1 :direction :back) ["d" data4]))
      (is (= (db-cursor-next cur1 :direction :forward) ["e" data5]))
      (db-cursor-delete cur1)
      (is (= (db-cursor-next cur1 :direction :back) ["d" data4]))
      (is (= (db-cursor-next cur1 :direction :forward) []))
      (db-cursor-put cur1 "e" data5)
      (db-cursor-replace cur1 data1)
      (db-cursor-next cur1 :direction :back)
      (is (= (db-cursor-next cur1 :direction :forward) ["e" data1]))
      (is (= (db-cursor-current cur1) ["e" data1]))
      (is (= (db-cursor-first cur1) ["a" data1]))
      (is (= (db-cursor-last cur1) ["e" data1])))))


(deftest indices
  (let [data1 {:id 1 :first-name "Aardvark" :last-name "Aardvarkov"}
        data2 {:id 2 :first-name "Baran" :last-name "Baranovich"}
        data3 {:id 3 :first-name "Beleg" :last-name "Cuthalion"}]
    (with-db-sec idx1 [*db-env* *db* "idx1" :allow-create true :key-creator-fn :last-name]
      (db-put *db* 1 data1)
      (db-put *db* 2 data2)
      (db-put *db* 3 data3)
      (is (= (db-get *db* 1) [1 data1]))
      (is (= (db-sec-get idx1 "Cuthalion") [3 data3]))
      (is (= (db-sec-get idx1 "Balkonsky") []))
      (db-sec-delete idx1 "Baranovich")
      (is (= (db-get *db* 2) []))
      (is (= (db-sec-get idx1 "Baranovich") [])))))


(deftest index-cursors
  (let [data1 {:id 1 :first-name "Aardvark" :last-name "Aardvarkov"}
        data2 {:id 2 :first-name "Baran" :last-name "Baranovich"}
        data3 {:id 3 :first-name "Beleg" :last-name "Cuthalion"}]
    (with-db-sec idx1 [*db-env* *db* "idx1" :allow-create true :key-creator-fn :last-name]
      (db-put *db* 1 data1)
      (db-put *db* 2 data2)
      (db-put *db* 3 data3)
      (with-db-cursor cur1 [idx1]
        (is (= (db-cursor-search cur1 "Baran") [2 data2]))
        (is (= (db-cursor-next cur1) [3 data3]))))))


(deftest joins
  (let [car-1  {:marque "BMW"   :model "X3"       :year 2006 :color "blue"  :awd true}
        car-2  {:marque "Audi"  :model "TT"       :year 2002 :color "blue"  :awd true}
        car-3  {:marque "Audi"  :model "allroad"  :year 2002 :color "grey"  :awd true}
        car-4  {:marque "Audi"  :model "A6 Avant" :year 2006 :color "white" :awd true}
        car-5  {:marque "Audi"  :model "Q5"       :year 2010 :color "white" :awd true}
        car-6  {:marque "Ford"  :model "Taurus"   :year 1996 :color "beige" :awd false}
        car-7  {:marque "BMW"   :model "330i"     :year 2003 :color "blue"  :awd false}
        car-8  {:marque "Eagle" :model "Summit"   :year 1994 :color "blue"  :awd false}
        car-9  {:marque "Audi"  :model "A4"       :year 2003 :color "blue"  :awd false}]
    (with-db-sec idx-color [*db-env* *db* "idx-color"
                            :allow-create true :sorted-duplicates true
                            :key-creator-fn :color]
      (with-db-sec idx-year [*db-env* *db* "idx-year"
                             :allow-create true :sorted-duplicates true
                             :key-creator-fn :year]
        (with-db-sec idx-awd [*db-env* *db* "idx-awd"
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
          (with-db-cursor c1 [idx-year]
            (with-db-cursor c2 [idx-color]
              (is (= (db-cursor-search c1 2003) [7 car-7]))
              (is (= (db-cursor-search c2 "blue") [1 car-1]))
              (with-db-join-cursor j [[c1 c2]]
                (is (= (db-join-cursor-next j) [7 car-7]))
                (is (= (db-join-cursor-next j) [9 car-9]))
                (is (= (db-join-cursor-next j) []))))))))))
