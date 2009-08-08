(ns cupboard.marshal
  (:import [com.sleepycat.je DatabaseEntry])
  (:import [com.sleepycat.bind.tuple TupleBinding TupleInput TupleOutput]))


(declare unmarshal-db-entry)                  ; TODO: Clean me up.


;; 100 -> [1 :integer 100]
;; 1.1 -> [1 :float 1.1]
;; "hello" -> [1 :string(utf-8) "hello"]
;; [1 2.2] -> [2 :integer 1 :float 2.2]
;; {:one 1 :two 2.2} -> [2 :kv ["one" :integer 1] :kv ["two" :float 2.2]]
;;
;; indexing


(def type-codes
     {java.lang.Boolean    1
      java.lang.Character  2
      java.lang.Byte       3
      java.lang.Short      4
      java.lang.Integer    5
      java.lang.Long       6
      java.math.BigInteger 7
      java.lang.Double     8
      java.lang.String     9})


(defmulti marshal-db-entry class)

(defmacro def-primitive-marshal-method [java-type write-method]
  `(defmethod marshal-db-entry ~java-type [data#]
     (let [db-entry#     (DatabaseEntry.)
           tuple-output# (TupleOutput.)]
       (.writeUnsignedByte tuple-output# (type-codes ~java-type))
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
