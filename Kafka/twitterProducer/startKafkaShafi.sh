#!/bin/bash
# start zookeeper
sudo ~/zookeeper-3.4.6/bin/zkServer.sh start

# start kafka daemon
sudo ~/kafka_2.9.2-0.8.1.1/bin/kafka-server-start.sh -daemon config/server.properties

# create twitter topic
~/kafka_2.9.2-0.8.1.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterStream

# create stockTwits topic
~/kafka_2.9.2-0.8.1.1/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic stockTwitsStream
