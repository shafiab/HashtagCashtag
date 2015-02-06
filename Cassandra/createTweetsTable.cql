DROP KEYSPACE IF EXISTS latesttweets;
CREATE KEYSPACE latesttweets WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

use latesttweets;

CREATE TABLE recenttweets
(
 id int,
 user text,
 tweet text,
 year int,
 month int,
 day int,
 hour int,
 minute int,
 second int,
 PRIMARY KEY ((id),year, month, day, hour, minute, second)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC, minute DESC, second DESC);


