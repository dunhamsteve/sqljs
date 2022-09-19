# Javascript SQLite toy

This project started out as an exercise to read a sqlite file and dump the table contents as a list of objects.  I wrote it to learn a little bit about how sqlite works.

Recently (2021) I decided to add support for reading indexes and running SQL queries. The code is still a little terse because I was trying to see how much I could do with a small amout of code.

SQLite requires you to parse the DDL to know the names of the columns. I started out with a regex hack in my initial version. This hack got replaced by a state machine that is a bit inscrutible and hard to modify. It will eventually be replaced by a parser, like one for SQL queries.

## Files

- src/sqlite.ts - code to read the sqlite file and do index scans.
- src/parser.ts - code to parse SELECT statements
- src/eval.ts   - query planning and execution
- src/types.ts  - shared types


## Tasks

- [ ] Switch the schema reader to a real parser
- [ ] Add pager layer for remote databases
- [ ] Improve query planning:
  - [ ] Add support for range scans (from/to)
  - [ ] Be smarter about which index to choose
  - [ ] Support some OR operations
  - [ ] Support left join
  - [ ] Translate constraints to use the tuple index (instead of looking up name on each access)
- [ ] Much later
  - [ ] Merge join? (we could return a value to the generator for the next key to seek)


## Query Planning

Flatten out the top level AND to get a list of constraints. It doesn't handle a top level OR well at the moment.

Current strategy is to topo-sort the tables. We grab the first index that matches some constraint and recurse to dependencies before doing the index scan.


The DDL parser is a bit weird - I was playing around with a state machine and code-golfing.  I'll rewrite that into the statement parser code at some point. 
