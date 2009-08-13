(ns test.cupboard.marshal
  (:use [clojure.contrib.test-is])
  (:use [cupboard.marshal]))


(deftest basic-marshaling
  (let [tboolean true
        tchar    \c
        tbyte    (byte 1)
        tshort   (short 1)
        tint     (int 1)
        tlong    (long 1)
        tbigint  (bigint 1)
        tratio   (/ 1 2)
        tdouble  1.0
        tstring  "hello world"
        tdate    (java.util.Date.)
        tkeyword :one
        tsymbol  'one
        tlist    (list 1 2 3)
        tvector  [1 2 3]
        tmap     {:one 1 :two 2 :three 3}
        tset     #{:one 2 'three}]
    (is (= (unmarshal-db-entry (marshal-db-entry tboolean)) tboolean))
    (is (= (unmarshal-db-entry (marshal-db-entry tchar)) tchar))
    (is (= (unmarshal-db-entry (marshal-db-entry tbyte)) tbyte))
    (is (= (unmarshal-db-entry (marshal-db-entry tshort)) tshort))
    (is (= (unmarshal-db-entry (marshal-db-entry tint)) tint))
    (is (= (unmarshal-db-entry (marshal-db-entry tlong)) tlong))
    (is (= (unmarshal-db-entry (marshal-db-entry tbigint)) tbigint))
    (is (= (unmarshal-db-entry (marshal-db-entry tratio)) tratio))
    (is (= (unmarshal-db-entry (marshal-db-entry tdouble)) tdouble))
    (is (= (unmarshal-db-entry (marshal-db-entry tstring)) tstring))
    (is (= (unmarshal-db-entry (marshal-db-entry tdate)) tdate))
    (is (= (unmarshal-db-entry (marshal-db-entry tkeyword)) tkeyword))
    (is (= (unmarshal-db-entry (marshal-db-entry tsymbol)) tsymbol))
    (is (= (unmarshal-db-entry (marshal-db-entry tlist)) tlist))
    (is (= (unmarshal-db-entry (marshal-db-entry tvector)) tvector))
    (is (= (unmarshal-db-entry (marshal-db-entry tmap)) tmap))
    (is (= (unmarshal-db-entry (marshal-db-entry tset)) tset))))
