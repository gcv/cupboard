(ns cupboard.bdb.je-marshal
  (:use cupboard.utils)
  (:import [org.joda.time DateTime LocalDate LocalTime LocalDateTime DateTimeZone])
  (:import [com.sleepycat.je DatabaseEntry OperationStatus]
           [com.sleepycat.bind.tuple TupleBinding TupleInput TupleOutput]))


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
                  org.joda.time.DateTime
                  org.joda.time.LocalDate
                  org.joda.time.LocalTime
                  org.joda.time.LocalDateTime
                  java.util.UUID
                  clojure.lang.Keyword
                  clojure.lang.Symbol
                  :list
                  :vector
                  :seq
                  :map
                  :set])

(def *clj-type-codes* (zipmap *clj-types* (range 0 (count *clj-types*))))


(defn clj-type [data]
  (condp #(%1 %2) data
    map? :map
    set? :set
    list? :list
    vector? :vector
    seq? :seq
    (class data)))


(defmulti marshal-write (fn [tuple-output data] (clj-type data)))

(defmacro def-marshal-write [java-type write-method]
  `(defmethod marshal-write ~java-type [#^TupleOutput tuple-output#
                                        #^{:tag ~java-type} data#]
     (.writeUnsignedByte tuple-output# (*clj-type-codes* ~java-type))
     (when-not (nil? data#) (~write-method tuple-output# data#))))

(def-marshal-write nil (fn [_] nil))
(def-marshal-write java.lang.Boolean .writeBoolean)
(def-marshal-write java.lang.Boolean .writeBoolean)
(def-marshal-write java.lang.Character
  (fn [#^TupleOutput tuple-output #^java.lang.Character data]
    (.writeChar tuple-output (int data))))
(def-marshal-write java.lang.Byte .writeByte)
(def-marshal-write java.lang.Short .writeShort)
(def-marshal-write java.lang.Integer .writeInt)
(def-marshal-write java.lang.Long .writeLong)
(def-marshal-write java.math.BigInteger .writeBigInteger)
(def-marshal-write clojure.lang.Ratio
  (fn [#^TupleOutput tuple-output #^clojure.lang.Ratio data]
    (marshal-write tuple-output (.numerator data))
    (marshal-write tuple-output (.denominator data))))
(def-marshal-write java.lang.Double .writeSortedDouble)
(def-marshal-write java.lang.String .writeString)
(def-marshal-write java.util.Date
  (fn [#^TupleOutput tuple-output #^java.util.Date data]
    (.writeString tuple-output (str (.withZone (DateTime. data) DateTimeZone/UTC)))))
(def-marshal-write DateTime
  (fn [#^TupleOutput tuple-output #^DateTime data]
    (.writeString tuple-output (str (.withZone data DateTimeZone/UTC)))))
(def-marshal-write LocalDate
  (fn [#^TupleOutput tuple-output #^LocalDate data]
    (.writeString tuple-output (str data))))
(def-marshal-write LocalTime
  (fn [#^TupleOutput tuple-output #^LocalTime data]
    (.writeString tuple-output (str data))))
(def-marshal-write LocalDateTime
  (fn [#^TupleOutput tuple-output #^LocalDateTime data]
    (.writeString tuple-output (str data))))
(def-marshal-write java.util.UUID
  (fn [#^TupleOutput tuple-output #^java.util.UUID uuid]
    (.writeLong tuple-output (.getMostSignificantBits uuid))
    (.writeLong tuple-output (.getLeastSignificantBits uuid))))
(def-marshal-write clojure.lang.Keyword
  (fn [#^TupleOutput tuple-output #^clojure.lang.Keyword data]
    (.writeString tuple-output (.substring (str data) 1))))
(def-marshal-write clojure.lang.Symbol
  (fn [#^TupleOutput tuple-output #^clojure.lang.Symbol data]
    (.writeString tuple-output (str data))))
(letfn [(seq-write [tuple-output data]
          (marshal-write tuple-output (count data))
          (doseq [e data] (marshal-write tuple-output e)))]
  (def-marshal-write :list seq-write)
  (def-marshal-write :vector seq-write)
  (def-marshal-write :seq seq-write)
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

(defn marshal-db-entry*
  "A helper function for making optionally-empty DatabaseEntry objects from
   keyword argument maps."
  [map-arg key-arg]
  (if (contains? map-arg key-arg)
      (marshal-db-entry (map-arg key-arg))
      (DatabaseEntry.)))


(defmulti unmarshal-read
  (fn [#^TupleInput tuple-input]
    (let [type-byte (.readUnsignedByte tuple-input)]
      (nth *clj-types* type-byte))))

(defmacro def-unmarshal-read [java-type read-method]
  `(defmethod unmarshal-read ~java-type [#^TupleInput tuple-input#]
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
  (fn [#^TupleInput tuple-input]
    (java.util.Date. (long (.getMillis (DateTime. (.readString tuple-input)))))))
(def-unmarshal-read DateTime
  (fn [#^TupleInput tuple-input] (DateTime. (.readString tuple-input))))
(def-unmarshal-read LocalDate
  (fn [#^TupleInput tuple-input] (LocalDate. (.readString tuple-input))))
(def-unmarshal-read LocalTime
  (fn [#^TupleInput tuple-input] (LocalTime. (.readString tuple-input))))
(def-unmarshal-read LocalDateTime
  (fn [#^TupleInput tuple-input] (LocalDateTime. (.readString tuple-input))))
(def-unmarshal-read java.util.UUID
  (fn [#^TupleInput tuple-input] (java.util.UUID.
                                  (.readLong tuple-input)
                                  (.readLong tuple-input))))
(def-unmarshal-read clojure.lang.Keyword
  (fn [#^TupleInput tuple-input] (keyword (.readString tuple-input))))
;; XXX: Symbols get interned in the package which unmarshals the symbol!!!
(def-unmarshal-read clojure.lang.Symbol
  (fn [#^TupleInput tuple-input] (symbol (.readString tuple-input))))
;; TODO: When available in a full Clojure release, use a transient data
;; structure to put together sequences in the loop (as is, it creates quite a
;; bit of garbage). Alternatively, maybe use a for form if it is more efficient?
;; http://clojure.org/transients
(letfn [(seq-read-fn [starting-value after-fn]
          (fn [tuple-input]
            (let [len (unmarshal-read tuple-input)]
              (loop [i 0 res starting-value]
                (if (>= i len)
                    (after-fn res)
                    (recur (inc i) (conj res (unmarshal-read tuple-input))))))))]
  (def-unmarshal-read :list (seq-read-fn (list) reverse))
  (def-unmarshal-read :seq (seq-read-fn (list) reverse))
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

(defn unmarshal-db-entry [#^DatabaseEntry db-entry]
  (if (= 0 (.getSize db-entry))
      nil
      (let [tuple-input (TupleBinding/entryToInput db-entry)]
        (unmarshal-read tuple-input))))

(defn unmarshal-db-entry*
  "A helper function which returns a [key data] pair given the result of a
   retrieval operation and the corresponding DatabaseEntry objects."
  [result key-entry data-entry]
  (if (= result OperationStatus/SUCCESS)
      [(unmarshal-db-entry key-entry) (unmarshal-db-entry data-entry)]
      []))
