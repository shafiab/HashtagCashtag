# -*- coding: utf-8 -*-
"""
Created on Sun Feb  1 12:16:24 2015

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
        date = '20150202' #dt.datetime.now().strftime("%Y%m%d")
        #if dt.datetime.now().hour >14 or dt.datetime.now().hour<6:
        #    time.sleep(3600) # sleep for an hour at night
            
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
	#with open('file3', 'w') as out:
    	#	for line in lineset:
        #		out.write(line)
	#cmd = ("comm -13 <(sort %s) <(sort %s) > %s" % (fileNameOld, fileNameNew, fileNameTmp))
        #cmd = ("comm -13 %s %s > %s"  % (fileNameOld, fileNameNew, fileNameTmp))
        #print cmd
	#code = os.system(cmd)    
        
	#print code
        #if code==0: #success
            # move new file to old file
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
	    	#fileTmp.close()
	   # time.sleep(300) # sleep for 5mins		
    
if __name__ == "__main__":
    doWork(sys.argv[1], sys.argv[2])


                


#comm -13 AAPLOld.csv AAPLNew.csv > diff.txt
