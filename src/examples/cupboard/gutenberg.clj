(ns examples.cupboard.gutenberg
  (:use [clojure.contrib java-utils pprint])
  (:require [cupboard.core :as cb])
  (:use [cupboard.utils])
  (:require [clojure.xml :as xml])
  (:require [clojure.contrib.lazy-xml :as lazy-xml]))



;;; ----------------------------------------------------------------------------
;;; Cupboard demonstration
;;; ----------------------------------------------------------------------------

;;; Declare a database-persisted struct-map. This creates a real struct-map with
;;; the given slot names, and with the given indices.
(cb/defpersist etext
  ((:id :index :unique)
   (:title :index :any)
   (:author :index :any)
   (:language :index :any)
   (:date-added :index :any)))


;;; Given a list of raw entry maps, write them to the database. This is done
;;; using cb/make-instance, which both instantiates a database-persisted
;;; struct-map and writes it to the database.
(defn write-entries [entries]
  (cb/with-txn [:no-sync true]
    (doseq [entry entries]
      (cb/make-instance etext [(entry :id)
                               (entry :title)
                               (entry :author)
                               (entry :language)
                               (entry :date-added)]))))


;;; Start by downloading the Project Gutenberg catalog:
;;; http://www.gutenberg.org/wiki/Gutenberg:Feeds
;;; As of [2009-10-11 Sun], the direct link is:
;;; http://www.gutenberg.org/feeds/offline-package.tar.bz2
;;; You need the large .rdf file for this example's raw data.

;;; Open the default cupboard, cb/*cupboard*. To speed up the initial data
;;; import, disable the cleaner and checkpointer threads.
;; (cb/open-cupboard! "/tmp/gutenberg-catalog" :run-cleaner false :run-checkpointer false)

;;; The data import takes roughly 70 seconds on a mid-2008 MacBook Pro with 4GB
;;; of RAM, with the Java heap size set to 2GB and the concurrent mark-and-sweep
;;; garbage collector turned on (-server -Xms2000M -Xmx2000M -XX:+UseConcMarkSweepGC).
;;; There should be ways to tune the import process and make it faster.
;; (time (import-catalog "/tmp/gutenberg-catalog.rdf"))

;;; Turn the cleaner and checkpointer threads back on.
;; (cb/modify-env :run-cleaner true :run-checkpointer true)

;;; Check the number of imported books. It should exceed 30000.
;; (cb/shelf-count)

;;; List all books written in Afrikaans.
;; (pprint
;;  (cb/query (= :language "af")))

;;; Change the language code for all Afrikaans books from "af" to "Afrikaans".
;; (pprint
;;  (cb/query (= :language "af") :callback #(cb/passoc! % :language "Afrikaans")))

;;; Now, delete all books written in Afrikaans.
;; (cb/query (= :language "Afrikaans") :callback cb/delete)

;;; Note the decremented count of the number of entries on the default shelf.
;; (cb/shelf-count)

;;; Demonstrate joins and restoring the struct-map types of the results. This
;;; finds all of Émile Zola's originals (not translations) in the catalog,
;;; specifically "J'accuse!.." and returns the result as an etext struct-map.
;; (let [result (cb/query (= :language "fr")
;;                        (starts-with :author "Zola")
;;                        (starts-with :title "J'accuse")
;;                        :struct etext)
;;       book (first result)]
;;   (pprint book)
;;   (pprint (type book)))

;;; Demonstrate retrieving only a subset of entries from the database.
;; (pprint
;;  (cb/query (= :language "fr") (starts-with :author "Dumas") :limit 3))

;;; Demonstrate transaction rollback.
;; (cb/with-txn []
;;   (println "starting shelf count:" (cb/shelf-count))
;;   (cb/query (= :language "fr") (starts-with :author "Dumas")
;;             :callback cb/delete)
;;   (println "shelf count after deletion:" (cb/shelf-count))
;;   (cb/rollback)
;;   (println "shelf count after rollback:" (cb/shelf-count)))

;;; Close the default cupboard.
;; (cb/close-cupboard!)



;;; ----------------------------------------------------------------------------
;;; forward declarations
;;; ----------------------------------------------------------------------------

(declare get-etext etexts gutenberg-catalog)



;;; ----------------------------------------------------------------------------
;;; Project Gutenberg catalog extraction
;;; (no Cupboard-specific code; just XML parsing)
;;; ----------------------------------------------------------------------------

(defn import-catalog [catalog-path]
  (System/setProperty "entityExpansionLimit" (str Integer/MAX_VALUE))
  (let [writer-agent (agent [])]
    (doseq [new-entry (etexts (gutenberg-catalog catalog-path lazy-xml/parse-trim))]
      (send writer-agent
            (fn [curr-val]
              (let [new-val (conj curr-val (get-etext new-entry))]
                (if (< (count curr-val) 9) ; 10 at a time
                    ;; then: just accumulate
                    new-val
                    ;; else: write to database
                    (do
                      (write-entries new-val)
                      []))))))
    ;; write whatever is left in the agent's queue
    (send writer-agent (fn [curr-val] (write-entries curr-val)))
    (println "done, waiting for agent")
    (await writer-agent)))


(defn gutenberg-catalog [location parser-fn]
  (let [location (file location)
        full-xml-seq (parser-fn location)
        content-seq (full-xml-seq :content)]
    content-seq))


(defn etexts [content-seq]
  (filter #(= (:tag %) :pgterms:etext) content-seq))


(defn extract-element [element]
  (cond
    ;; title
    (and (= (element :tag) :dc:title)
         (= (-> element :attrs :rdf:parseType) "Literal"))
    [:title (first (-> element :content))]
    ;; author
    (and (= (element :tag) :dc:creator)
         (= (-> element :attrs :rdf:parseType) "Literal"))
    [:author (first (-> element :content))]
    ;; language
    (= (element :tag) :dc:language)
    [:language (let [content (element :content)
                     all-lang (map #(first ((first (% :content)) :content)) content)]
                 (if (= (count all-lang) 1) (first all-lang) all-lang))]
    ;; date added
    (= (element :tag) :dc:created)
    [:date-added (let [content (element :content)
                       all-dates (map #(first ((first (% :content)) :content)) content)
                       date (localdate (first all-dates))]
                   date)]
    :else nil))


(defn get-etext [entry]
  (let [id (-> entry :attrs :rdf:ID)
        content (entry :content)
        literals (remove nil? (map extract-element content))]
    (apply hash-map (flatten (concat [:id id] literals)))))
