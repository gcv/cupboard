# Cupboard



## Basic Concepts

* **cupboard**: corresponds to the concept of a database. A `cupboard` must be
  opened in a particular directory on a filesystem. Internally implemented as a
  Berkeley DB environment. `cupboard` objects contain `shelf` objects.
* **shelf**: contains objects and indices on those objects. Multiple shelves may
  be convenient for some applications. Other applications might just store
  everything on the default shelf.
* **index**: allows rapid access to an object stored on a shelf by the value of
  a particular slot in that object. Indices may be `:unique`, in which case
  insertion of a duplicate results in an error, or `:any`, in which case
  duplicates are permitted.
* **transaction**: creates an atomic unit of work to be performed on a
  `cupboard`. A transaction may affect multiple shelves. Transactions can be
  committed or rolled back. Berkeley DB transactions support deadlock detection,
  and, Cupboard transactions will attempt to retry in case of
  deadlock. Therefore, code wrapped in a transaction should be free of all side
  effects (except for those affecting the database).



## API Concepts

In the interest of clarity, all examples here use `cb` as an alias of the
`cupboard.core` package:

    (ns application.package
      (:require [cupboard.core :as cb]))

Most functions of the Cupboard API have keyword parameters. Most of these
keywords are optional.

In particular, when working with the default cupboard, the default shelf, and
the default transaction, `:cupboard`, `:shelf-name`, and `:txn` keyword
arguments, respectively, should be omitted. Unless stated otherwise, all
functions in the Cupboard API support these keywords, and have the following
meanings:

* `:cupboard`: if omitted, use the dynamic variable `cupboard.core/*cupboard*`
  as the cupboard for the function.
* `:shelf-name`: if omitted, use the default shelf name. This default name is
  given by `cupboard.core/*default-shelf-name*`.
* `:txn`: if omitted, use the dynamic variable `cupboard.core/*txn*` as the
  transaction variable for the function.



## Cupboards

Functions which manipulate cupboards do not support the default `:cupboard`,
`:shelf-name`, and `:txn` keywords because they do not make sense in this context.

* `(cb/open-cupboard path)` opens a cupboard at the specified path and returns it.
* `(cb/open-cupboard! path)` opens a cupboard at the specified path, and sets
  `cupboard.core/*cupboard*` to its value.
* `(cb/close-cupboard cupboard-var)` closes the cupboard referred to by `cupboard-var`.
* `(cb/close-cupboard!)` closes the default cupboard, `cupboard.core/*cupboard*`.
* `(cb/with-open-cupboard [path] body-forms)` makes sure that the
  body forms execute inside an open default cupboard, and closes it when done. Be
  careful with spawning threads in side the body, since the scope of the body
  may end before the threads end their work.
* `(cb/with-open-cupboard [cupboard-var path] body-forms)` works as above, but binds the
  open cupboard to `cupboard-var`.

A cupboard may be opened for read-only access using the `:read-only` keyword:

* `(cb/open-cupboard! "/tmp/example" :read-only true)`
* `(cb/with-open-cupboard [my-cupboard "/tmp/example" :read-only true] ...)`



## Objects

### Defining

Use the `cb/defpersist` macro to define objects, e.g.:

    (cb/defpersist object-name
      ((:slot-1 :index :unique)
       (:slot-2 :index :any)
       (:slot-3)))

Here, `:slot-1` has a unique index, `:slot-2` has a non-unique index, and
`:slot-3` has no index at all. Non-indexed slots may not be used for query
clauses.

Slot names should be Clojure keywords.

`cb/defpersist` expands into Clojure's `defstruct` form, so all objects use
indexed addressing of structs, rather than hashed addressing of hash-maps.


### Instantiating

Use the `cb/make-instance` multimethod to instantiate objects defined using
`cb/defpersist`, e.g.:

* `(cb/make-instance object-name [value-1 value-2 value-3 ...])`

This form will instantiate the object, write it to the default shelf of the
default open cupboard, and return it.

* `(cb/make-instance object-name [value-1 value-2 value-3] :save false)` will
  not write the object. This is typically only useful for testing.

Newly-created objects have metadata which describes how Cupboard persists
them. Of some interest is the `:primary-key` entry in the object's metadata; it
is currently implemented as a randomly-generated UUID.


### Modifying

If you hold on to a reference of an object created using `cb/make-instance`, or
retrieved using `cb/retrieve`, you may write it directly. This relies on the
`:primary-key` metadata entry of the object.

* `(cb/save object)`

Since modifying objects and saving them to the database should be fairly common,
Cupboard provides helper functions for doing so. `cb/passoc!` adds the given
key-value pair, or multiple pairs, to the given object, saves, and returns
it. `cb/pdissoc!` removes keys and saves the object.

* `(cb/passoc! object new-key new-value)`
* `(cb/passoc! object [new-key-1 new-value-1 new-key-2 new-value-2 ...])`
* `(cb/pdissoc! object key)`
* `(cb/pdissoc! object [key-1 key-2 ...])`

An object may be also be removed:

* `(cb/delete object)`

`cb/passoc!`, `cb/pdissoc!`, `cb/delete` are most useful in query callbacks.


### Retrieving

Objects in Cupboard may only be retrieved by indexed slot values.

* `(cb/retrieve index-slot indexed-value)`

`cb/retrieve` returns an object directly when retrieved using a unique
index. Otherwise, it falls back on `cb/query` and returns a lazy sequence of
objects.



## Shelves

* `(cb/remove-shelf shelf-name :cupboard cupboard-var)` deletes the given shelf
  by name.
* `(cb/list-shelves :cupboard cupboard-var)` returns a list of all shelves on
  the given cupboard.
* `(cb/shelf-count :cupboard cupboard-var :shelf-name shelf-name)` counts the
  number of objects on the given shelf.
* `(cb/clear-shelf :cupboard cupboard-var :shelf-name shelf-name)` deletes all
  objects on the given shelf.



## Transactions

* `(cb/begin-txn)` returns an open transaction. It must be either
  committed or rolled back.
* `(cb/commit)` commits an open transaction.
* `(cb/rollback)` rolls back an open transaction.
* `(cb/with-txn [options] body-forms)` executes body forms in an open
  transaction and commits it, unless `(cb/rollback)` is executed somewhere in
  the body.

Deadlocks are part of life when dealing with Berkeley DB transactions, and
Cupboard tries to handle them gracefully. In particular, you may specify the
following options to `cb/with-txn`:

* `:max-attempts` specifies how many times a transaction retries if it runs into
  a deadlock. Default: 5.
* `:retry-delay-msec` specifies how long to wait before retrying a deadlocked
  transaction. Default: 10 msec.

When deadlock occurs inside a `cb/with-txn` form, the transaction rolls back,
waits out the specified delay, and tries again, up to `:max-attempts` times. If
this does not work, Cupboard throws an exception.

Since code inside `cb/with-txn` may retry, it must not have any side effects!



## Queries

Cupboard supports picking out a set of objects by a set of criteria from a
particular shelf.

* `(cb/query clause*)`

Each clause is an s-expression:

* `(rule-fn indexed-slot value)`

`rule-fn` are functions from the following sets:

* From `clojure.core`: `= < <= > >=`
* From `cupboard.utils'`: `starts-with date= date> date>= date< date<=`

For example:

    (cb/query (starts-with :name "J")
              (>= :age 25)
              (< :age 30))

Two notes about performance:

1. Note that only using `=` clauses, i.e., performing a natural join, yields the
   best performance, e.g., `(cb/query (= :key1 val1) (= :key2 val2)`.
2. When using ranges anywhere in the query, try to order the clauses in such a
   way that the first clause reduces the result set as much as possible. Due to
   limitations of JE, Cupboard cannot determine the optimal order by
   itself. (This will hopefully be fixed in a future version of JE.)

If no clauses are specified, `cb/query` returns the entire contents of the
shelf. Be careful.

`cb/query` has two important keyword parameters.

* `:limit` reduces the number of returned entries to the given number.
* `:callback` specifies an optional function to be called on each object in the
  query's result set.

`:callback` allows deleting and updating elements in shelves, provided they meet
the query's criteria. Examples:

    (cb/query (date< :registered (localdate "2009-01-01"))
              :callback cb/delete)
    (cb/query (date>= :registered (localdate "2009-06-01"))
              :callback #(cb/passoc! % :status :new))

Note that queries using explicit `:cupboard`, `:shelf-name`, and `:txn` values
must explicitly close over those values in the callback definition:

    (cb/query (date= :registered (localdate "2009-09-01"))
              :cupboard my-cupboard
              :shelf-name "users"
              :callback #(cb/passoc! % :status :sept1
                                       :cupboard my-cupboard
                                       :shelf-name "users"))



## Hints and Warnings

* A shelf may contain multiple object types, but doing so requires care. If
  their indices have overlapping slot names (e.g., if they all have `:login`
  slots), then queries may return objects of multiple types.
* Saving an object with a new index defined on it may take time, since other
  objects on that shelf will have to be scanned to see if they need to be added
  to that index.
* Deleting an index currently requires using `cupboard.bdb.je` functions. This
  will be fixed in a future version.
* Cupboard uses Joda Time dates. `cupboard.utils/localdate` returns a simple
  date without a time zone attached. `cupboard.utils/localtime` returns a simple
  time. `cupboard.utils/datetime` returns a full timestamp, with millisecond
  precision and a time zone. `cupboard.utils/localdatetime` returns a full
  timestamp, with millisecond precision, but no time zone. All accept date and
  time strings in ISO 8601 format.
* Do not mix types in indexed values. In other words, an index on slot :id
  should always contain values of the same type, not a mixture of strings and
  integers, for example. The order of the values `"123"` and `123` relative to
  each other is not defined in Cupboard.
* Do not make indices on slots containing Clojure `hash-map` values. Although
  Cupboard will save such a slot containing a hash map, the lack of ordering
  guarantees will lead to unexpected results when attempting to retrieve by that
  index.
