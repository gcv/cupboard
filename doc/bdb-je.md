# Using [Berkeley DB JE](http://www.oracle.com/database/berkeley-db/je/index.html) From Clojure

`cupboard.bdb.je` provides a thin Clojure wrapper for much of JE's
functionality: environments, primary databases, secondary databases,
transactions, cursors, and join cursors. Use with care: it provides no error
handling beyond that provided by JE itself. You are responsible for closing
cursors, transactions, databases, and so on. The main Cupboard API is higher
level and generally better suited for application programming.


## Marshalling Data

`cupboard.bdb.je` automatically marshals most Clojure types into the kinds of
byte streams Berkeley DB writes to disk. It uses Berkeley DB's Bind APIs to do
this. Oracle's documentation claims that these APIs outperform Java's
serialization.

Aside from Clojure types, Cupboard also marshals Joda Time date and time
types. See `cupboard.bdb.je-marshal` for details.
