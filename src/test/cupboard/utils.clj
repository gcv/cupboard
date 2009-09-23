(ns test.cupboard.utils
  (:use [cupboard utils])
  (:use [clojure.contrib test-is]))


(deftest args-map-test
  (let [a1v [:one 1 :two 2]             ; test as a vector
        a1m {:one 1 :two 2}             ; test as a map
        a2  {:three 3 :four 4}
        expected {:one 1 :two 2 :three 3 :four 4}]
    (is (= (merge (args-map a1v) a2) expected))
    (is (= (merge (args-map a1m) a2) expected))
    (is (= (merge (args-map [a1m]) a2) expected))))


(deftest args-&rest-&keys-test
  (let [c1 [1 2 3 :a 4 :b 5]            ; [1 2 3], {:a 4 :b 5}
        c2 [:a 4 :b 5 1 2 3]            ; [1 2 3], {:a 4 :b 5}
        c3 [:a 4 :b 5]                  ; [], {:a 4 :b 5}
        c4 [1 2 3]                      ; [1 2 3], {}
        c5 [:a 4 1 2 3 :b 5]            ; [1 2 3], {:a 4 :b 5}
        c6 [1 2 :a 4 :b 5 3]]           ; [1 2 3], {:a 4 :b 5}
    (is (= (args-&rest-&keys c1) [[1 2 3] {:a 4 :b 5}]))
    (is (= (args-&rest-&keys c2) [[1 2 3] {:a 4 :b 5}]))
    (is (= (args-&rest-&keys c3) [[] {:a 4 :b 5}]))
    (is (= (args-&rest-&keys c4) [[1 2 3] {}]))
    (is (= (args-&rest-&keys c5) [[1 2 3] {:a 4 :b 5}]))
    (is (= (args-&rest-&keys c6) [[1 2 3] {:a 4 :b 5}]))))


(deftest deref*-test
  (let [x (atom {:one 1 :two 2})]
    (is (= (deref* x) @x))
    (is (nil? (deref* nil)))))


(deftest date-routines
  (let [d1  (java.util.Date.)
        ds1 (date->iso8601 d1 :msec true)
        ds2 "2009-08-13 01:34:04.666Z"
        d2 (iso8601->date ds2)
        ds3 "2009-08-13 01:34:04Z"
        d3 (iso8601->date ds3)]
    (is (= (iso8601->date ds1) d1))
    (is (= (date->iso8601 d2 :msec true) ds2))
    (is (= (date->iso8601 d3) ds3))))


(deftest flattening
  (is (= (flatten '[1 2 (3 [4] 5 "fred") [6 7]]) [1 2 3 4 5 "fred" 6 7])))
