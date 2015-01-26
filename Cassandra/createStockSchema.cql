DROP KEYSPACE IF EXISTS stockData;
CREATE KEYSPACE stockData WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

use stockData;


CREATE TABLE minuteStock (
   ticker text,
   year int,
   month int,
   day int,
   hour int,
   minute int,
   high double,
   low double,
   open double,
   close double,
   volume double,
   PRIMARY KEY ((ticker), year, month, day, hour, minute)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC, minute DESC);

CREATE TABLE hourStock (
   ticker text,
   year int,
   month int,
   day int,
   hour int,
   high double,
   low double,
   open double,
   close double,
   volume double,
   PRIMARY KEY ((ticker), year, month, day, hour)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);

CREATE TABLE dayStock (
   ticker text,
   year int,
   month int,
   day int,
   high double,
   low double,
   open double,
   close double,
   volume double,
   PRIMARY KEY ((ticker), year, month, day)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC);

CREATE TABLE weekStock (
   ticker text,
   year int,
   week int,
   high double,
   low double,
   open double,
   close double,
   volume double,
   PRIMARY KEY ((ticker), year, week)
) WITH CLUSTERING ORDER BY (year DESC, week DESC);

CREATE TABLE monthStock (
   ticker text,
   year int,
   month int,
   high double,
   low double,
   open double,
   close double,
   volume double,
   PRIMARY KEY ((ticker), year, month)
) WITH CLUSTERING ORDER BY (year DESC, month DESC);


CREATE TABLE yearStock (
   ticker text,
   year int,
   high double,
   low double,
   open double,
   close double,
   volume double,
   PRIMARY KEY ((ticker), year)
) WITH CLUSTERING ORDER BY (year DESC);