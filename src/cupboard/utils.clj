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



;; ----------------------------------------------------------------------
;; date handling routines
;; ----------------------------------------------------------------------

(def *iso8601-date-format*
     (doto (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss'Z'")
       (.setTimeZone (TimeZone/getTimeZone "UTC"))))

(def *iso8601-date-format-millis*
     (doto (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss.SSS'Z'")
       (.setTimeZone (TimeZone/getTimeZone "UTC"))))

(defn date->iso8601 [#^Date date & opts-args]
  (let [defaults {:millis false}
        opts     (merge defaults (args-map opts-args))]
    (if (opts :millis)
        (.format *iso8601-date-format-millis* date)
        (.format *iso8601-date-format* date))))

(defn iso8601->date [datestr]
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
  (let [dir (file dir)]
    (when-not (.exists dir)
      (throw (FileNotFoundException.
              (str "Not found for deletion: " (.getAbsolutePath dir)))))
    (when (.isDirectory dir)
      (doseq [x (.list dir)] (rmdir-recursive (File. dir x))))
    (when-not (.delete dir)
      (throw (IOException. (str "Failed to delete " (.getAbsolutePath dir)))))))
