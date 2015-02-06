#!/usr/bin/env python
import os
import re
import sys
import logging


import socket
import time

def get_lock(process_name):
    global lock_socket
    lock_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        lock_socket.bind('\0' + process_name)
        print 'Lock acquired'
    except socket.error:
        print 'Process already running. Exiting..'
        sys.exit()

get_lock('stock streaming')



logging.basicConfig()

from kafka import KafkaClient, SimpleConsumer
from datetime import datetime
from docopt import docopt

kafka = KafkaClient("localhost:9092")

tempfile_path = None
tempfile = None
batch_counter = 0
timestamp = None

# def get_topics(zookeeper_hosts, topic_regex):
#     """Uses shell zookeeper-client to read Kafka topics matching topic_regex from ZooKeeper."""
#     command        = "/usr/bin/zookeeper-client -server %s ls /brokers/topics | tail -n 1 | tr '[],' '   '" % ','.join(zookeeper_hosts)
#     topics         = os.popen(command).read().strip().split()
#     matched_topics = [ topic for topic in topics if re.match(topic_regex, topic) ]
#     return matched_topics

def standardized_timestamp(frequency, dt=None):
    '''
    This function generates a timestamp with predictable minute and seconds
    components. Right now, we hardcode seconds to 0. The minutes component 
    is more interesting. For Oozie coordinator we need to have a predictable 
    timestamp component so we can predict how future input paths will look
    like. That's why we can't rely on a 'real' timestamp because the minutes
    and seconds are arbitrary.
    If dt is not given, then the a standarized timestamp based on the current
    time will be returned.  Else the standardized timestamp of dt will be
    returned.
    @param frequency (integer) that indicates how to collapse minutes. 
    For example frequency=15
    10:06 -> 10:00
    10:23 -> 10:15
    10:59 -> 10:45
    Frequency=30
    10:21 -> 10:00
    10:49 -> 10:30
    
    Frequency=0 is a special case used to generate daily filenames.
    2013-01-04 11:18 -> 2013-01-04
    2013-01-05 00:01 -> 2013-01-05
    '''

    if dt is None:
      dt = datetime.now() 

    frequency = int(frequency)
    # Special case were frequency=0 so we only return the date component
    if frequency == 0:
        return dt.strftime('%Y-%m-%d')

    blocks = 60 / frequency
    standardized_minutes = {}
    for block in xrange(blocks):
        standardized_minutes[block] = block * frequency

    collapsed_minutes = (dt.minute / frequency)
    minutes = standardized_minutes.get(collapsed_minutes, 0)
    timestamp = datetime(dt.year, dt.month, dt.day, dt.hour, minutes, 0)

    return timestamp.strftime('%Y%m%d%H%M%S')

def flush_to_hdfs(output_dir, topic):
    global tempfile_path, tempfile, batch_counter
    tempfile.close()
    hadoop_dir = "%s/%s" % (output_dir, topic)
    hadoop_path = hadoop_dir + "/%s_%s.dat" % (timestamp, batch_counter)
    print "/usr/bin/hdfs dfs -mkdir %s 2> /dev/null" % hadoop_dir
    os.system("/usr/bin/hdfs dfs -mkdir %s 2> /dev/null" % hadoop_dir)
    print "/usr/bin/hdfs dfs -put -f %s %s 2> /dev/null" % (tempfile_path, hadoop_path)
    os.system("/usr/bin/hdfs dfs -put -f %s %s 2> /dev/null" % (tempfile_path, hadoop_path))
    os.remove(tempfile_path)
    batch_counter += 1
    tempfile_path = "/tmp/kafka_%s_%s_%s_%s.dat" % (topic, group, timestamp, batch_counter)
    tempfile = open(tempfile_path,"w")

def consume_topic(topic, group, output_dir, frequency):
    global timestamp, tempfile_path, tempfile
    print "Consuming from topic '%s' in consumer group %s into %s..." % (topic, group, output_dir)

    #get timestamp
    timestamp = standardized_timestamp(frequency)
    kafka_consumer = SimpleConsumer(kafka, group, topic, max_buffer_size=1310720000)
    
    #open file for writing
    tempfile_path = "/tmp/kafka_stockData_%s_%s_%s_%s.dat" % (topic, group, timestamp, batch_counter)
    tempfile = open(tempfile_path,"w")
    log_has_at_least_one = False #did we log at least one entry?
    while True:
        messages = kafka_consumer.get_messages(count=1000, block=False) #get 5000 messages at a time, non blocking
        if not messages:
	       os.system("sleep 300s") # sleep 5mins
	       continue
           
        for message in messages: #OffsetAndMessage(offset=43, message=Message(magic=0, attributes=0, key=None, value='some message'))
            log_has_at_least_one = True
            #print(message.message.value)
            tempfile.write(message.message.value + "\n")
        if tempfile.tell() > 10000000: #10000000: #file size > 10MB
            flush_to_hdfs(output_dir, topic)
        kafka_consumer.commit() #save position in the kafka queue
    #exit loop
    if log_has_at_least_one:
        flush_to_hdfs(output_dir, topic)
    kafka_consumer.commit() #save position in the kafka queue
    return 0


if __name__ == '__main__':
    group           = "stockData"
    output          = "/user/data"
    topic           = "stock"
    frequency       = "1"
    
    print "\nConsuming topic: [%s] into HDFS" % topic
    consume_topic(topic, group, output, frequency)
    kafka.close()
    sys.exit(0)
