(ns cupboard.utils
  (:use [clojure.contrib java-utils])
  (:import [java.io File IOException FileNotFoundException]
           [java.text SimpleDateFormat ParseException]
           [java.util Date TimeZone]))



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
  (let [rests         (atom [])
        keys          (atom {})
        args-size     (count args)
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


(defmacro defstruct*
  "Wrapper for defstruct which also creates a simple duck type
  checking function."
  [name & slots]
  `(do
     (defstruct ~name ~@slots)
     (defn ~(symbol (str "is-" name "?")) [x#]
       (if (map? x#)
           (let [sample# (struct ~name)]
             (empty? (clojure.set/difference
                      (set (keys sample#)) (set (keys x#)))))
           false))))


(defn any? [pred coll]
  (if (seq coll)
      (if (pred (first coll))
          true
          (recur pred (next coll)))
      false))



;; ----------------------------------------------------------------------
;; date handling routines
;; ----------------------------------------------------------------------

(def #^SimpleDateFormat *iso8601-date-format*
     (doto (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss'Z'")
       (.setTimeZone (TimeZone/getTimeZone "UTC"))))

(def #^SimpleDateFormat *iso8601-date-format-millis*
     (doto (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss.SSS'Z'")
       (.setTimeZone (TimeZone/getTimeZone "UTC"))))

(defn date->iso8601
  {:tag String}
  [#^Date date & opts-args]
  (let [defaults {:millis false}
        opts     (merge defaults (args-map opts-args))]
    (if (opts :millis)
        (.format *iso8601-date-format-millis* date)
        (.format *iso8601-date-format* date))))

(defn iso8601->date
  {:tag Date}
  [#^String datestr]
  (try
   (.parse *iso8601-date-format-millis* datestr)
   (catch ParseException pe
     (.parse *iso8601-date-format* datestr))))



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
