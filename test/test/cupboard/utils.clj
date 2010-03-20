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


(deftest flattening
  (is (= (flatten '[1 2 (3 [4] 5 "fred") [6 7]]) [1 2 3 4 5 "fred" 6 7])))


(deftest date-comparison
  (let [dt1 (datetime)
        dt2 (datetime "2009-01-01T15:00")
        dt3 (datetime "2008-01-01T15:00")]
    (is (date= dt1 dt1))
    (is (date< dt3 dt2))
    (is (date> dt2 dt3))
    (is (date<= dt1 dt1))
    (is (date<= dt3 dt2))
    (is (date>= dt1 dt1))
    (is (date>= dt2 dt3))))
