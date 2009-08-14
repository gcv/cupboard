(ns cupboard.marshal
  (:use [cupboard.utils])
  (:import [com.sleepycat.je DatabaseEntry])
  (:import [com.sleepycat.bind.tuple TupleBinding TupleInput TupleOutput]))


;; TODO: Verify that strings encode in UTF-8 under all circumstances.
;; TODO: Lock out lazy Clojure data structures if possible?
;; TODO: Handle Java collections?


(def *clj-types* [nil
                  java.lang.Boolean
                  java.lang.Character
                  java.lang.Byte
                  java.lang.Short
                  java.lang.Integer
                  java.lang.Long
                  java.math.BigInteger
                  clojure.lang.Ratio
                  java.lang.Double
                  java.lang.String
                  java.util.Date
                  clojure.lang.Keyword
                  clojure.lang.Symbol
                  :list
                  :vector
                  :map
                  :set])

(def *clj-type-codes* (zipmap *clj-types* (range 0 (count *clj-types*))))


(defn clj-type [data]
  (condp #(%1 %2) data
    map? :map
    set? :set
    list? :list
    vector? :vector
    (class data)))


(defmulti marshal-write (fn [tuple-output data] (clj-type data)))

(defmacro def-marshal-write [java-type write-method]
  `(defmethod marshal-write ~java-type [tuple-output# data#]
     (.writeUnsignedByte tuple-output# (*clj-type-codes* ~java-type))
     (when-not (nil? data#) (~write-method tuple-output# data#))))

(def-marshal-write nil (fn [_] nil))
(def-marshal-write java.lang.Boolean .writeBoolean)
(def-marshal-write java.lang.Boolean .writeBoolean)
(def-marshal-write java.lang.Character
  (fn [tuple-output data] (.writeChar tuple-output (int data))))
(def-marshal-write java.lang.Byte .writeByte)
(def-marshal-write java.lang.Short .writeShort)
(def-marshal-write java.lang.Integer .writeInt)
(def-marshal-write java.lang.Long .writeLong)
(def-marshal-write java.math.BigInteger .writeBigInteger)
(def-marshal-write clojure.lang.Ratio
  (fn [tuple-output data]
    (marshal-write tuple-output (.numerator data))
    (marshal-write tuple-output (.denominator data))))
(def-marshal-write java.lang.Double .writeSortedDouble)
(def-marshal-write java.lang.String .writeString)
(def-marshal-write java.util.Date
  (fn [tuple-output data]
    (.writeString tuple-output (date->iso8601 data :millis true))))
(def-marshal-write clojure.lang.Keyword
  (fn [tuple-output data] (.writeString tuple-output (subs (str data) 1))))
(def-marshal-write clojure.lang.Symbol
  (fn [tuple-output data] (.writeString tuple-output (str data))))
(letfn [(seq-write [tuple-output data]
          (marshal-write tuple-output (count data))
          (doseq [e data] (marshal-write tuple-output e)))]
  (def-marshal-write :list seq-write)
  (def-marshal-write :vector seq-write)
  (def-marshal-write :set seq-write))
(def-marshal-write :map
  (fn [tuple-output data]
    (marshal-write tuple-output (count data))
    (doseq [[key value] data]
      (marshal-write tuple-output key)
      (marshal-write tuple-output value))))

(defn marshal-db-entry
  "A general way to get a database entry from data. If data is a DatabaseEntry
   instance, just return it. If it is a supported type, convert it into a new
   DatabaseEntry object. If the optional db-entry-arg is passed in, reuse it
   as the target DatabaseEntry."
  [data & [db-entry-arg]]
  (if (instance? DatabaseEntry data)
      data
      (let [db-entry (if db-entry-arg db-entry-arg (DatabaseEntry.))
            tuple-output (TupleOutput.)]
        (marshal-write tuple-output data)
        (TupleBinding/outputToEntry tuple-output db-entry)
        db-entry)))


(defmulti unmarshal-read
  (fn [tuple-input]
    (let [type-byte (.readUnsignedByte tuple-input)]
      (nth *clj-types* type-byte))))

(defmacro def-unmarshal-read [java-type read-method]
  `(defmethod unmarshal-read ~java-type [tuple-input#]
     (~read-method tuple-input#)))

(def-unmarshal-read nil (fn [_] nil))
(def-unmarshal-read java.lang.Boolean .readBoolean)
(def-unmarshal-read java.lang.Character .readChar)
(def-unmarshal-read java.lang.Byte .readByte)
(def-unmarshal-read java.lang.Short .readShort)
(def-unmarshal-read java.lang.Integer .readInt)
(def-unmarshal-read java.lang.Long .readLong)
(def-unmarshal-read java.math.BigInteger .readBigInteger)
(def-unmarshal-read clojure.lang.Ratio
  (fn [tuple-input] (clojure.lang.Ratio.
                     (unmarshal-read tuple-input)
                     (unmarshal-read tuple-input))))
(def-unmarshal-read java.lang.Double .readSortedDouble)
(def-unmarshal-read java.lang.String .readString)
(def-unmarshal-read java.util.Date
  (fn [tuple-input] (iso8601->date (.readString tuple-input))))
(def-unmarshal-read clojure.lang.Keyword
  (fn [tuple-input] (keyword (.readString tuple-input))))
;; XXX: Symbols get interned in the package which unmarshals the symbol!!!
(def-unmarshal-read clojure.lang.Symbol
  (fn [tuple-input] (symbol (.readString tuple-input))))
;; TODO: When available, use a transient data structure to put
;; together sequences in the loop (as is, it creates quite a bit of
;; garbage). Alternatively, maybe use a for form if it is more
;; efficient?
(letfn [(seq-read-fn [starting-value after-fn]
          (fn [tuple-input]
            (let [len (unmarshal-read tuple-input)]
              (loop [i 0 res starting-value]
                (if (>= i len)
                    (after-fn res)
                    (recur (inc i) (conj res (unmarshal-read tuple-input))))))))]
  (def-unmarshal-read :list (seq-read-fn (list) reverse))
  (def-unmarshal-read :vector (seq-read-fn [] identity))
  (def-unmarshal-read :set (seq-read-fn #{} identity)))
(def-unmarshal-read :map
  (fn [tuple-input]
    (let [len (unmarshal-read tuple-input)]
      (loop [i 0 res (hash-map)]
        (if (>= i len)
            res
            (recur (inc i) (assoc res
                             (unmarshal-read tuple-input)
                             (unmarshal-read tuple-input))))))))

(defn unmarshal-db-entry [db-entry]
  (if (= 0 (.getSize db-entry))
      nil
      (let [tuple-input (TupleBinding/entryToInput db-entry)]
        (unmarshal-read tuple-input))))
