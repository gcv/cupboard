(ns cupboard.utils
  (:use [clojure.contrib java-utils])
  (:import [org.joda.time DateTime LocalDate LocalTime LocalDateTime DateTimeZone])
  (:import [java.io File IOException FileNotFoundException]
           [java.text SimpleDateFormat ParseException]))



;;; ----------------------------------------------------------------------
;;; plugging holes in Clojure
;;; ----------------------------------------------------------------------

(defn args-map [args]
  (cond
    ;; when called just on a map of arguments
    (map? args) args
    ;; when called on a map passed through a &rest parameter
    (and (sequential? args)
         (= (count args) 1)
         (map? (first args))) (first args)
    ;; when called on a vector or list
    :else (apply hash-map args)))


(defn args-&rest-&keys
  "Allows Clojure to parse Common Lisp style &rest &keys arguments, so
  function f could be invoked either as:
    (f req1 req2 req3 :opt1 opt1-val :opt2 opt2-val)
  or as
    (f :opt1 opt1-val :opt2 opt2-val req1 req2 req3)"
  [args]
  (let [rests (atom [])
        keys (atom {})
        args-size (count args)
        args-size-dec (dec args-size)]
    (cond (= args-size 0) [[] {}]
          (= args-size 1) [args {}]
          :else (loop [i 0]
                  (if (>= i args-size)
                      [@rests @keys]
                      (let [arg (nth args i)]
                        (if (and (keyword? arg) (< i args-size-dec))
                            ;; keyword argument with a value counterpart
                            (do (swap! keys assoc arg (nth args (inc i)))
                                (recur (+ i 2)))
                            (do (swap! rests conj arg)
                                (recur (inc i))))))))))


(defn any? [pred coll]
  (if (seq coll)
      (if (pred (first coll))
          true
          (recur pred (next coll)))
      false))


(defn deref*
  "Just like (deref ref), except it only dereferences non-nil refs."
  [ref]
  (when-not (nil? ref)
    (deref ref)))


(defmacro if*
  "Implements a Common Lisp style if form, with an implicit do (progn) in the
   else clause."
  [condition consequent-expr & alternate-body]
  (if alternate-body
      `(if ~condition ~consequent-expr (do ~@alternate-body))
      `(if ~condition ~consequent-expr)))


(defn starts-with [#^String haystack #^String needle]
  (if-not (nil? haystack)
    (.startsWith haystack needle)
    false))


(defn flatten [x]
  (let [s? #(instance? clojure.lang.Sequential %)]
    (filter (complement s?) (tree-seq s? seq x))))



;;; ----------------------------------------------------------------------
;;; Joda Date wrappers and supporting functions
;;; ----------------------------------------------------------------------

(defmacro make-jd-wrapper [name jd-type]
  `(defn ~name [& [x#]]
     (if (nil? x#) (new ~jd-type) (new ~jd-type x#))))

(make-jd-wrapper datetime DateTime)
(make-jd-wrapper localdate LocalDate)
(make-jd-wrapper localtime LocalTime)
(make-jd-wrapper localdatetime LocalDateTime)


(defn date= [d1 d2] (= (.compareTo d1 d2) 0))
(defn date< [d1 d2] (= (.compareTo d1 d2) -1))
(defn date> [d1 d2] (= (.compareTo d1 d2) 1))
(defn date<= [d1 d2] (let [v (.compareTo d1 d2)] (or (= v -1) (= v 0))))
(defn date>= [d1 d2] (let [v (.compareTo d1 d2)] (or (= v 1) (= v 0))))



;;; ----------------------------------------------------------------------
;;; file and directory routines
;;; ----------------------------------------------------------------------

(defn make-temp-dir []
  (let [tf (File/createTempFile "temp-" (str (java.util.UUID/randomUUID)))]
    (when-not (.delete tf)
      (throw (IOException.
              (str "Failed to delete temporary file " (.getAbsolutePath tf)))))
    (when-not (.mkdir tf)
      (throw (IOException.
              (str "Failed to create temporary directory " (.getAbsolutePath tf)))))
    tf))


(defn rmdir-recursive [dir]
  (let [#^File dir (file dir)]
    (when-not (.exists dir)
      (throw (FileNotFoundException.
              (str "Not found for deletion: " (.getAbsolutePath dir)))))
    (when (.isDirectory dir)
      (doseq [#^String x (.list dir)] (rmdir-recursive (File. dir x))))
    (when-not (.delete dir)
      (throw (IOException. (str "Failed to delete " (.getAbsolutePath dir)))))))
