# -*- coding: utf-8 -*-
"""
Created on Sat Jan 31 23:40:26 2015
www.netfonds.no does not provide data for all dates. This program supplements missing date from yahoo data
@author: shafiab
"""
from cassandra.cluster import Cluster
cluster = Cluster(['54.67.105.220'])
session = cluster.connect('stockdata')
from datetime import datetime

file = open('MSFT.csv')

for line in file:
    d = line.split(',')
    date = datetime.fromtimestamp(int(d[0]))
#    print date.year
#    INSERT INTO minutestock (user_id, first_name, last_name, emails)
#  VALUES('frodo', 'Frodo', 'Baggins', {'f@baggins.com', 'baggins@gmail.com'});
#  close,high,low,open,volume
    if date.day==27:
        session.execute("""INSERT INTO minutestock (ticker, year, month, day, hour, minute, close, high, low, open, volume) 
        	VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        	""",
        	("MSFT",date.year,date.month,date.day,date.hour,date.minute,float(d[1]),float(d[2]),float(d[3]),float(d[4]),float(d[5]))
        	)
        