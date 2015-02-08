# -*- coding: utf-8 -*-
"""
Created on Sun Feb  1 12:16:24 2015
this program fetch stock data from netfonds.no for a specific date 
@author: shafiab
"""
from kafka import *
from urllib import urlretrieve
import datetime as dt
import os
import time
import sys

# kafka setup
mykafka = KafkaClient("localhost:9092")
producer = SimpleProducer(mykafka)
topicName = 'stockData'
path = '/tmp/'


def doWork(ticker, exchange):
#    while True:
        date = '20150202'             
        fileNameNew = path+ticker+'New.csv'
        fileNameOld = path+ticker+'Old.csv'
        fileNameTmp = path+ticker+'Tmp.csv'
        url='http://hopey.netfonds.no/tradedump.php?date=%s&paper=%s.%s&csv_format=csv'
        print 'start retrieving' + ticker
	urlretrieve(url % (date, ticker,exchange), fileNameNew)
       	print 'finished retrieving' +ticker 
        
        # create old file if not exists
        if not os.path.isfile(fileNameOld):
	    print 'creating old file'
            fold = open(fileNameOld, "wb")
            fold.write("time,price,quantity,board,source,buyer,seller,initiator\n")
            fold.close()
            
        # find difference between old and new file
	#taking difference between old and new file
	with open(fileNameNew) as f1:
    	    lineset = set(f1)
	with open(fileNameOld) as f2:
		lineset.difference_update(f2)
	    print 'moving new file to old file'
            cmdMove= ("mv %s %s" % (fileNameNew, fileNameOld))
            os.system(cmdMove)
            
	    print 'start writing to Kafka...'
            #with open(fileNameTmp, "r") as fileTmp:
            for lineT in lineset:
                    line = lineT.split(',')
                    if len(line)==8: # check for correctness
                        line[0] = dt.datetime.strptime(line[0], "%Y%m%dT%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
                        newLine = [str(time.time()), ticker]
                        transformedLine = ','.join(newLine) +','+','.join(line)
			print transformedLine
                        producer.send_messages(topicName,transformedLine)
    
if __name__ == "__main__":
    doWork(sys.argv[1], sys.argv[2])


                


#comm -13 AAPLOld.csv AAPLNew.csv > diff.txt
