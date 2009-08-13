(ns test.cupboard.db-core
  (:use [clojure.contrib test-is profile])
  (:use [cupboard.db-core]))


(defn test-db-1 []
  (let [e1   (db-env-open "/tmp/1" :allow-create true)
        db1  (db-open e1 "db1" :allow-create true)
        data #{1 2 3 (list 4 5 6) [7 8 9] {:ten 10}}]
    (profile (dotimes [i 10000]
               (rput db1 (str i) data)))
    (profile (dotimes [i 10000]
               (rget db1 (str i))))
    (db-close db1)
    (db-env-close e1)))


(defn test-db-2 []
  (let [e1   (db-env-open "/tmp/2" :allow-create true)
        db1  (db-open e1 "db1" :allow-create true)
        data #{:one 1 :two 2 :three "three" :four "five six seven eight"}]
    (profile (dotimes [i 10000]
               (rput db1 (str i) data)))
    (profile (dotimes [i 10000]
               (rget db1 (str i))))
    (db-close db1)
    (db-env-close e1)))


(defn test-cursor-1 []
  (let [e1   (db-env-open "/tmp/3" :allow-create true)
        db1  (db-open e1 "db1" :allow-create true)
        data #{:one 1 :two 2 :three "three" :four "five six seven eight"}
        cur1 (db-cursor-open db1)]
    (rput db1 "one" data)
    (rget db1 "one")
    (db-cursor-close cur1)
    (db-close db1)
    (db-env-close e1)))
