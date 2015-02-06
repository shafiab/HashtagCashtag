
from flask import Flask, render_template, redirect, url_for, request, jsonify
from cassandra.cluster import Cluster
import time
from operator import itemgetter
import pandas as pd
import numpy as np
import time
import json
import datetime
import calendar
import random
import pytz
from pytz import timezone
utc=pytz.utc
eastern=pytz.timezone('US/Eastern')


app = Flask(__name__)

clusterTwitterSeries = Cluster(['54.67.105.220'])
sessionTwitterSeries = clusterTwitterSeries.connect('twitterseries')


clusterTopTrendingStreaming = Cluster(['54.67.105.220'])
sessionTopTrendingStreaming = clusterTopTrendingStreaming.connect('twittertrendingstreaming')


clusterTopTrending= Cluster(['54.67.105.220'])
sessionTopTrending= clusterTopTrendingStreaming.connect('twittertrending')

clusterStockData = Cluster(['54.67.105.220'])
sessionStockData = clusterStockData.connect('stockdata')

clusterTweets = Cluster(['54.67.105.220'])
sessionTweets = clusterTweets.connect('latesttweets')


@app.route('/')
@app.route('/welcome')
def welcome():
    url_for('static', filename='jquery.datetimepicker.css')
    url_for('static', filename='jquery.datetimepicker.js')
    return render_template("main.html")

@app.route('/live_streaming')
def live_streaming():
    data =[]
    rows = sessionTopTrendingStreaming.execute("select * from toptrending30min where id in (0,1,2,3,4) order by timestamp desc limit 5");
    

    freq = []
    ticker = []
    color1 = []

    for r in rows:
        if r.sentiment > 0:
            color = 'green'
        elif r.sentiment < 0:
            color = 'red'
        else:
            color ='blue'

        temp = [r.ticker, r.frequency, color, r.ticker]
        freq.append(r.frequency)
        ticker.append(r.ticker)
        color1.append(color)
        data.append(temp)
        
    index, frequencySorted = zip(*sorted(enumerate(freq), key=itemgetter(1)))
    data2 = []
    for i in index:
        temp = [ticker[i], freq[i], color1[i], ticker[i]]
        data2.append(temp)

    print data2
    text=[]
    rowsTweets = sessionTweets.execute("select * from recenttweets limit 10");
    for r1 in rowsTweets:
        #print r1.tweet
        text.append(r1.tweet)
    #print rows.text
    author = [r.user for r in rowsTweets]
    dateTime = [str(r.year)+'-'+str(r.month)+'-'+str(r.day)+' '+str(r.hour)+':'+str(r.minute)+':'+str(r.second) for r in rowsTweets]
    return jsonify(data=data2,text=text, author=author, dateTime=dateTime)   


@app.route('/live_streaming_tweets')
def live_streaming_tweets():
    text=[]
    rowsTweets = sessionTweets.execute("select * from recenttweets limit 10");
    for r1 in rowsTweets:
        #print r1.tweet
        text.append(r1.tweet)

    return jsonify(data=text)   

@app.route('/top_trending_hour/<datetime1>')
def top_trending_hour(datetime1):    
    date=datetime.datetime.strptime(datetime1,"%Y_%m_%d_%H")
    date_eastern=eastern.localize(date,is_dst=None)
    date_utc=date_eastern.astimezone(utc)
    datetime2 = date_utc.strftime('%Y_%m_%d_%H')
    dt = datetime2.split('_')

    print datetime1, dt
    
    st = "select * from toptrendinghour where year="+str(int(dt[0]))+" and month="+str(int(dt[1]))+" and day=" +str(int(dt[2]))+" and hour="+str(int(dt[3]))+" limit 10"
    print st
    data =[]
    rows = sessionTopTrending.execute(st)
    #"select * from toptrendinghour where year=2015 and month=1 and day=24 and hour=22 limit 10")
    for r in rows:
        if r.sentiment > 0:
            color = 'green'
        elif r.sentiment < 0:
            color = 'red'
        else:
            color ='blue'

        temp = [r.ticker, r.frequency, color, r.ticker]
        data.append(temp)      
    print data
    return jsonify(data=data)  

@app.route('/get_count_chart/<stockName>')
def get_count_chart(stockName):
    rowTime = sessionTwitterSeries.execute("select * from trendingminute where ticker= '" + stockName + "' ")
    data1 = []
    data2  = []
    for r in rowTime:    
        a = [calendar.timegm(datetime.datetime(r.year, r.month, r.day,r.hour,r.minute).timetuple())*1000, r.frequency]

        ss = 0
        if r.sentiment > 0:
            ss = 1
        else:
            if r.sentiment <0:
                ss = -1

        b = [calendar.timegm(datetime.datetime(r.year, r.month, r.day,r.hour,r.minute).timetuple())*1000, ss]
        data1.append(a)
        data2.append(b)

    data1.reverse()
    data2.reverse()
    text = 'Number of tweets/sentiment for '+stockName

    return jsonify(data1=data1, data2=data2, text=text)   


@app.route('/get_correlation_chart/<stockName>')
def get_correlation_chart(stockName):
    rows = sessionStockData.execute("select * from minutestock where ticker= '" + stockName + "' ")
    data = []
    print rows
    for r in rows:    
        #a = [time.mktime(datetime.datetime(r.year, r.month, r.day).timetuple())*1000, r.open, r.high,  r.low, r.close]
        timestamp1 = timezone('US/Pacific').localize(datetime.datetime(r.year, r.month, r.day,r.hour,r.minute))
        #a = [time.mktime(datetime.datetime(r.year, r.month, r.day,r.hour,r.minute).timetuple())*1000, r.open, r.high,  r.low, r.close]

        a = [calendar.timegm(timestamp1.astimezone(timezone('UTC')).timetuple())*1000, r.open, r.high,  r.low, r.close]
        data.append(a)

    data.reverse()

    rowTime = sessionTwitterSeries.execute("select * from trendingminute where ticker=  '" + stockName + "' ")
    data1 = []
    for r in rowTime:    
        #a = [calendar.timegm(datetime.datetime(r.year, r.month, r.day).timetuple())*1000, r.frequency]
        a = [calendar.timegm(datetime.datetime(r.year, r.month, r.day,r.hour,r.minute).timetuple())*1000, r.frequency]
        data1.append(a)
    data1.reverse()

    text = 'Stock Price/ Mentions for '+stockName

    return jsonify(data1=data, data2=data1, text=text)   
    
if __name__ == '__main__':
    app.run(debug=True)
