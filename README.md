Cupboard is an embedded database library for Clojure, intended to make writing
Clojure objects to disk, and retrieving them, a relatively simple task. Cupboard
is almost completely schemaless, supports unique and non-unique indices,
querying by one or more indices, and ACID transactional semantics.

At its lowest level, Cupboard uses Berkeley DB JE, and contains a fairly
complete Clojure wrapper for the JE API. This API is available in the
`cupboard.bdb.je` package; some projects may find this code useful on its
own. The `cupboard.core` package provides a higher-level abstraction.



## Sample Code

This code makes some struct-maps representing books, saves them into a database,
and updates them.

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

This example is available in `examples/examples/cupboard/simple.clj`.
`examples/examples/cupboard/gutenberg.clj` uses a few more of Cupboard's features.



## Dependencies

* Clojure (version 1.2.0 tested)
* clojure-contrib (version 1.2.0-compatible)
* Berkeley DB JE (version 4.0.92 tested)
* Joda Time (version 1.6.x tested)

Use Leiningen to download Cupboard's dependencies. Alternatively, Cupboard's
`build.xml` contains an Ant task which downloads its dependencies, including
Clojure and clojure-contrib.



## Getting Started

1. Clone the Cupboard Git repository.
2. Run `ant package`. This pulls down all dependencies and puts them into the
   `lib` directory. Then it compiles Cupboard's source into Java class files and
   packages everything into a jar file.
3. Copy the Cupboard jar and its dependencies to wherever you normally copy your
   Clojure dependencies. Make sure to add them to your project's classpath.

Note: Full Leiningen support is forthcoming once Cupboard is more stable.



## Documentation

Cupboard's documentation is still a work in progress. Please refer to the `doc`
directory in the source distribution.



## License

Cupboard is distributed under the MIT license, and so has few restrictions
itself. Its Berkeley DB dependency, however, is licensed by Oracle &mdash; see
[Oracle's Berkeley DB Licensing Information
page](http://www.oracle.com/technology/software/products/berkeley-db/htdocs/licensing.html)
for details.
