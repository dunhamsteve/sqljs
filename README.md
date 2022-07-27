# Javascript SQLite toy

This project started out as a quick exercise to read a sqlite file, dumping contents as a list of object.  To learn about some sqlite internals.

I've recently (2021) added handling of indices and the beginnings of SQL select statement parsing and execution. 

The DDL parser is a bit weird - I was playing around with a state machine and code-golfing.  I'll rewrite that into the statement parser code at some point. 
