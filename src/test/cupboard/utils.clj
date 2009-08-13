(ns test.cupboard.utils
  (:use [clojure.contrib.test-is])
  (:use [cupboard.utils]))


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
