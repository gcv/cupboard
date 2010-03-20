(ns examples.cupboard.simple
  (:use clojure.contrib.pprint)
  (:require [cupboard.core :as cb])
  (:use [cupboard.utils]))

(cb/open-cupboard! "/tmp/books")

(cb/defpersist book
  ((:isbn :index :unique)
   (:title :index :any)
   (:author :index :any)))

(cb/make-instance book ["0393039854", "The First Folio", "Shakespeare, William"])
(cb/make-instance book ["0393925870", "The Canterbury Tales", "Chaucer"])
(cb/make-instance book ["143851557X", "Troilus and Criseyde", "Chaucer"])
(cb/make-instance book ["019280619X", "Paradise Lost", "Milton, John"])

(println "this returns maps")
(pprint (cb/query (starts-with :isbn "039")))

(println "this returns struct-map values")
(pprint (cb/query (= :author "Chaucer") :struct book))

(println "this updates the :author slot")
(cb/query (= :author "Chaucer")
          :callback #(cb/passoc! % :author "Chaucer, Geoffrey"))

(println "this verifies that the update worked")
(pprint (cb/query (starts-with :author "Chaucer")))

(cb/close-cupboard!)
