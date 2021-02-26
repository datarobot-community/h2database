# A fork of [H2](http://www.h2database.com/) database with custom features implementation

## The custom features are:

* User-defined tables and schemas
* Double.NaN and NULL are used interchangeably
* Date arithmetics (date + 1, timestamp + 1)
* Infinite.NaN in aggregate functions is the same as NaN
* Character single space is the same as NULL
* UPDATE statement returns the number of changed rows (non ANSI SQL compliant)
* Integer division is done as floats.
* 0/0 is NULL
* User-defined functions can specify the precision of the result
* Disable three-way logic (non ANSI SQL compliant)
* Linked views in addition to linked tables
* User-defined context for database connection
* User-defined automatic type conversion
* Calculated aliases
* User-defined listeners for linked queries execution
* User-defined collaterals
* User-defined mapping between database types and H2 types for linked tables
