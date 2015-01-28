DROP KEYSPACE IF EXISTS twitterTrending;
CREATE KEYSPACE twitterTrending WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

use twitterTrending;

CREATE TABLE topTrendingHour (
   index int,
   year int,
   month int,
   day int,
   hour int,
   ticker text,
   sentiment int,
   PRIMARY KEY ((index), year, month, day, hour)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);



CREATE TABLE topTrendingDay (
   index int,
   year int,
   month int,
   day int,   
   ticker text,
   sentiment int,
   PRIMARY KEY ((index), year, month, day)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC);



CREATE TABLE topTrendingWeek (
   index int,
   year int,
   week int,
   ticker text,
   sentiment int,
   PRIMARY KEY ((index), year, week)
) WITH CLUSTERING ORDER BY (year DESC, week DESC);



CREATE TABLE topTrendingMonth (
   index int,
   year int,
   month int,
   ticker text,
   sentiment int,
   PRIMARY KEY ((index), year, month)
) WITH CLUSTERING ORDER BY (year DESC, month DESC);


CREATE TABLE topTrendingYear (
   index int,
   year int,
   ticker text,
   sentiment int,
   PRIMARY KEY ((index), year)
) WITH CLUSTERING ORDER BY (year DESC);
