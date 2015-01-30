DROP KEYSPACE IF EXISTS twitterTrending;
CREATE KEYSPACE twitterTrending WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

use twittertrendingstreaming;

CREATE TABLE toptrending30min
(
 ticker text,
 id int,
 timestamp int,
 frequency int,
 sentiment int,
 PRIMARY KEY((timestamp),id)
 ) WITH CLUSTERING ORDER BY (id ASC);

CREATE TABLE topTrendingHour (
   year int,
   month int,
   day int,
   hour int,
   frequency int,
   ticker text,
   sentiment int,
   PRIMARY KEY ((year, month, day, hour), frequency)
) WITH CLUSTERING ORDER BY (frequency DESC);



CREATE TABLE topTrendingDay (
   year int,
   month int,
   day int,   
   frequency int,
   ticker text,
   sentiment int,
   PRIMARY KEY ((year, month, day), frequency)
) WITH CLUSTERING ORDER BY (frequency DESC);



CREATE TABLE topTrendingWeek (
   year int,
   week int,
   frequency int,
   ticker text,
   sentiment int,
   PRIMARY KEY ((year, week), frequency)
) WITH CLUSTERING ORDER BY (frequency DESC);



CREATE TABLE topTrendingMonth (
   year int,
   month int,
   ticker text,
   frequency int,
   sentiment int,
   PRIMARY KEY ((year, month), frequency)
) WITH CLUSTERING ORDER BY (frequency DESC);


CREATE TABLE topTrendingYear (
   year int,
   ticker text,
   frequency int,
   sentiment int,
   PRIMARY KEY ((year), frequency)
) WITH CLUSTERING ORDER BY (frequency DESC);
