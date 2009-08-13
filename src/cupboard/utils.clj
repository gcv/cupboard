(ns cupboard.utils
  (:import [java.text SimpleDateFormat ParseException])
  (:import [java.util Date TimeZone]))



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
        opts     (merge defaults (apply hash-map opts-args))]
    (if (opts :millis)
        (.format *iso-date-format-millis* date)
        (.format *iso-date-format* date))))

(defn iso8601->date [datestr]
 (try
  (.parse *iso8601-date-format-millis* datestr)
  (catch ParseException pe
    (.parse *iso8601-date-format* datestr))))
