(ns test.cupboard.utils
  (:use [cupboard utils])
  (:use [clojure.contrib test-is]))


(defstruct* duck-typed-struct :a :b :c)


(deftest args-map-test
  (let [a1v [:one 1 :two 2]             ; test as a vector
        a1m {:one 1 :two 2}             ; test as a map
        a2  {:three 3 :four 4}
        expected {:one 1 :two 2 :three 3 :four 4}]
    (is (= (merge (args-map a1v) a2) expected))
    (is (= (merge (args-map a1m) a2) expected))
    (is (= (merge (args-map [a1m]) a2) expected))))


(deftest duck-typed-predicates
  (let [dt1     (struct duck-typed-struct 1 2 3)
        dt2     {:a 4 :b 5 :c 6}
        dt3     (struct duck-typed-struct 1 2)
        dt4     {:a 4 :b 5 :c nil}
        not-dt1 {:a 4 :b 5}
        not-dt2 {:d 7 :e 8}
        not-dt3 {:a 4 :b 5 :d 7}]
    (is (is-duck-typed-struct? dt1))
    (is (is-duck-typed-struct? dt2))
    (is (is-duck-typed-struct? dt3))
    (is (is-duck-typed-struct? dt4))
    (is (not (is-duck-typed-struct? not-dt1)))
    (is (not (is-duck-typed-struct? not-dt2)))
    (is (not (is-duck-typed-struct? not-dt3)))))


(deftest date-routines
  (let [d1  (java.util.Date.)
        ds1 (date->iso8601 d1 :millis true)
        ds2 "2009-08-13 01:34:04.666Z"
        d2 (iso8601->date ds2)
        ds3 "2009-08-13 01:34:04Z"
        d3 (iso8601->date ds3)]
    (is (= (iso8601->date ds1) d1))
    (is (= (date->iso8601 d2 :millis true) ds2))
    (is (= (date->iso8601 d3) ds3))))
