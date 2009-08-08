(ns cupboard.marshal
  (:import [com.sleepycat.je DatabaseEntry])
  (:import [com.sleepycat.bind.tuple TupleBinding TupleInput TupleOutput]))


;; TODO: This needs a test suite.


;; TODO: Verify that strings encode in UTF-8 under all circumstances.
;; TODO: Handle Clojure lists, vectors, maps, and sets.
;; TODO: Lock out lazy Clojure data structures if possible?
;; TODO: Handle Java collections?


;; TODO: Verify the correctness of using array-map here and the
;; performance of the lookups (all linear, might be good for such a
;; small map).
(def type-codes-by-type
     (array-map java.lang.Boolean    1
                java.lang.Character  2
                java.lang.Byte       3
                java.lang.Short      4
                java.lang.Integer    5
                java.lang.Long       6
                java.math.BigInteger 7
                java.lang.Double     8
                java.lang.String     9
                clojure.lang.Keyword 10
                clojure.lang.Symbol  11))

(def type-codes-by-code
     (zipmap (vals type-codes-by-type) (keys type-codes-by-type)))


(defmulti marshal-db-entry class)

(defmacro def-primitive-marshal-method [java-type write-method]
  `(defmethod marshal-db-entry ~java-type [data#]
     (let [db-entry#     (DatabaseEntry.)
           tuple-output# (TupleOutput.)]
       (.writeUnsignedByte tuple-output# (type-codes-by-type ~java-type))
       (~write-method tuple-output# data#)
       (TupleBinding/outputToEntry tuple-output# db-entry#)
       db-entry#)))

(def-primitive-marshal-method java.lang.Boolean .writeBoolean)
(def-primitive-marshal-method java.lang.Character
  (fn [tuple-output data] (.writeChar tuple-output (int data))))
(def-primitive-marshal-method java.lang.Byte .writeByte)
(def-primitive-marshal-method java.lang.Short .writeShort)
(def-primitive-marshal-method java.lang.Integer .writeInt)
(def-primitive-marshal-method java.lang.Long .writeLong)
(def-primitive-marshal-method java.math.BigInteger .writeBigInteger)
(def-primitive-marshal-method java.lang.Double .writeSortedDouble)
(def-primitive-marshal-method java.lang.String .writeString)


(defmulti unmarshal-db-entry-helper (fn [native-type _] native-type))

(defmacro def-primitive-unmarshal-helper-method [java-type read-method]
  `(defmethod unmarshal-db-entry-helper ~java-type [native-type# tuple-input#]
     (~read-method tuple-input#)))

(def-primitive-unmarshal-helper-method java.lang.Boolean .readBoolean)
(def-primitive-unmarshal-helper-method java.lang.Character .readChar)
(def-primitive-unmarshal-helper-method java.lang.Byte .readByte)
(def-primitive-unmarshal-helper-method java.lang.Short .readShort)
(def-primitive-unmarshal-helper-method java.lang.Integer .readInteger)
(def-primitive-unmarshal-helper-method java.lang.Long .readLong)
(def-primitive-unmarshal-helper-method java.math.BigInteger .readBigInteger)
(def-primitive-unmarshal-helper-method java.lang.Double .readSortedDouble)
(def-primitive-unmarshal-helper-method java.lang.String .readString)

(defn unmarshal-db-entry [db-entry]
  (let [tuple-input (TupleBinding/entryToInput db-entry)
        type-byte   (.readUnsignedByte tuple-input)
        native-type (type-codes-by-code type-byte)]
    (unmarshal-db-entry-helper native-type tuple-input)))
