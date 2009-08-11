(ns cupboard.marshal
  (:import [com.sleepycat.je DatabaseEntry])
  (:import [com.sleepycat.bind.tuple TupleBinding TupleInput TupleOutput]))


;; TODO: This needs a test suite.


;; TODO: Verify that strings encode in UTF-8 under all circumstances.
;; TODO: Handle Clojure lists, vectors, maps, and sets.
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
                  java.lang.Double
                  java.lang.String
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
     (~write-method tuple-output# data#)))

(def-marshal-write java.lang.Boolean .writeBoolean)
(def-marshal-write java.lang.Boolean .writeBoolean)
(def-marshal-write java.lang.Character
  (fn [tuple-output data] (.writeChar tuple-output (int data))))
(def-marshal-write java.lang.Byte .writeByte)
(def-marshal-write java.lang.Short .writeShort)
(def-marshal-write java.lang.Integer .writeInt)
(def-marshal-write java.lang.Long .writeLong)
(def-marshal-write java.math.BigInteger .writeBigInteger)
(def-marshal-write java.lang.Double .writeSortedDouble)
(def-marshal-write java.lang.String .writeString)
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

(defn marshal-db-entry [data]
  (let [db-entry     (DatabaseEntry.)
        tuple-output (TupleOutput.)]
    (marshal-write tuple-output data)
    (TupleBinding/outputToEntry tuple-output db-entry)
    db-entry))


(defmulti unmarshal-read
  (fn [tuple-input]
    (let [type-byte (.readUnsignedByte tuple-input)]
      (nth *clj-types* type-byte))))

(defmacro def-unmarshal-read [java-type read-method]
  `(defmethod unmarshal-read ~java-type [tuple-input#]
     (~read-method tuple-input#)))

(def-unmarshal-read java.lang.Boolean .readBoolean)
(def-unmarshal-read java.lang.Character .readChar)
(def-unmarshal-read java.lang.Byte .readByte)
(def-unmarshal-read java.lang.Short .readShort)
(def-unmarshal-read java.lang.Integer .readInt)
(def-unmarshal-read java.lang.Long .readLong)
(def-unmarshal-read java.math.BigInteger .readBigInteger)
(def-unmarshal-read java.lang.Double .readSortedDouble)
(def-unmarshal-read java.lang.String .readString)
(def-unmarshal-read clojure.lang.Keyword
  (fn [tuple-input] (keyword (.readString tuple-input))))
;; XXX: Symbols get interned in the package which unmarshals the symbol!!!
(def-unmarshal-read clojure.lang.Symbol
  (fn [tuple-input] (symbol (.readString tuple-input))))
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

(defn unmarshal-db-entry [db-entry]
  (let [tuple-input (TupleBinding/entryToInput db-entry)]
    (unmarshal-read tuple-input)))
