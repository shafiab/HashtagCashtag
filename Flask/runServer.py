
from flask import Flask, render_template, redirect, url_for, request, jsonify
from cassandra.cluster import Cluster
import time
import pandas as pd
import numpy as np
import time
import json
import datetime

app = Flask(__name__)
cluster = Cluster(['54.67.105.220'])
session = cluster.connect('stockdata')

#println rows
#session = cluster.connect('test')

@app.route('/')
@app.route('/welcome')
def welcome():
    return render_template("test.html")

@app.route('/getGraph/<stock>')
def showStockChart(stock, chartID = 'chart_ID', chart_type = 'StockChart', chart_height = 350):

    rows = session.execute("select * from minutestock where ticker= '" + stock + "' ")
    data = []
    for r in rows:    
        a = [time.mktime(datetime.datetime(r.year, r.month, r.day,r.hour,r.minute).timetuple())*1000, r.open, r.high,  r.low, r.close]
        data.append(a)

    data.reverse()

    text = stock + ' stock price'
    
 
    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
    #series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
    #title = {"text": 'My Title'}
    #xAxis = {"categories": ['xAxis Data1', 'xAxis Data2', 'xAxis Data3']}
    #yAxis = {"title": {"text": 'yAxis Label'}}
    return render_template('stockChart.html', data=json.dumps(data), text=json.dumps(text))


@app.route('/graph_ex')
def index(chartID = 'chart_ID', chart_type = 'StockChart', chart_height = 350):


    rows = session.execute("select * from minutestock where ticker='FB'")
    data = []
    for r in rows:    
        a = [time.mktime(datetime.datetime(r.year, r.month, r.day,r.hour,r.minute).timetuple())*1000, r.open, r.high,  r.low, r.close]
        data.append(a)

    data.reverse()


    title= { "text": 'AAPL stock price by minute'}
    series =[{
                "name" : 'AAPL',
                "type": 'candlestick',
                "data" : data,
                "tooltip": {
                    "valueDecimals": 2
                }}]
                

    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
    #series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
    #title = {"text": 'My Title'}
    #xAxis = {"categories": ['xAxis Data1', 'xAxis Data2', 'xAxis Data3']}
    #yAxis = {"title": {"text": 'yAxis Label'}}
    return render_template('index1.html', data=json.dumps(data))


# @app.route('/login', methods=['GET', 'POST'])
# def login():
#     error = None
#     if request.method == 'POST':
#         if request.form['username'] != "admin" or request.form['password'] != 'admin':
#             error = 'Invalid credentials'
#         else:
#             return redirect(url_for('home'))
#     return render_template("login.html", error=error)


# @app.route('/county/')
# def county_full():
#     start_time = time.time()
#     county_full = session.execute('SELECT * FROM by_county_full')
#     print time.time() - start_time

#     start_time = time.time()
#     counties = ""    
#     for row in county_full:
#         counties += row.county + ": " + str(row.cnt) + "<br>"
#     print time.time() - start_time

#     return counties

# @app.route('/monthly')
# def county_month(chartID = 'chart_ID2', chart_type = 'line', chart_height = 350):    
#     start_time = time.time()
#     county = "Dallas County"
#     county_month = session.execute("SELECT * FROM by_county_month WHERE county = '" + county + "'")
#     print time.time() - start_time

#     start_time = time.time()
#     counties_month = ""

#     def date_to_milli(time_tuple):
#         epoch_sec = time.mktime((1970, 1, 1, 0, 0, 0, 0, 0, 0))
#         return 1000*int(time.mktime(time_tuple) - epoch_sec)


#     data = []
#     for row in county_month:
#         data.append([date_to_milli((row.year, row.month, 0, 0, 0, 0, 0, 0, 0)), row.cnt])        
#         counties_month += row.county + ": (" + str(row.year) + "," + str(row.month) + ") " + str(row.cnt) + "<br>"
#     print time.time() - start_time



#     chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
#     series = [{"name": county, "data": data}]
#     title = {"text": "Meetups in " + county + " each month"}
#     xAxis = {"type": 'datetime', 
#              #"dateTimeLabelFormats": {"month": '%e. %b',
#              #                         "year":  '%b'},
#              "title": {"text": 'Date'}
#              }
#     yAxis = {"title": {"text": '# of Meetups'},
#              "min": 0
#              }
#     return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)



#     #return counties_month

# @app.route('/daily/<county>/')
# def county_day(county):   
#     start_time = time.time()
#     county_day = session.execute("SELECT * FROM by_county_day WHERE county = '" + county + "'")
#     print time.time() - start_time

#     start_time = time.time()
#     counties_day = ""    
#     for row in county_day:
#         counties_day += row.county + ": (" + str(row.year) + "," + str(row.month) + "," + str(row.day) + ") " + str(row.cnt) + "<br>"
#     print time.time() - start_time

#     return counties_day

# @app.route('/hourly/<county>/')
# def county_hour(county):  
#     start_time = time.time()
#     county_hour = session.execute("SELECT * FROM by_county_hour WHERE county = '" + county + "'")
#     print time.time() - start_time

#     start_time = time.time()
#     counties_hour = ""    
#     for row in county_hour:
#         counties_hour += row.county + ": (" + str(row.year) + "," + str(row.month) + "," + str(row.day) + "," + str(row.hour) + ") " + str(row.cnt) + "<br>"
#     print time.time() - start_time

#     return counties_hour


if __name__ == '__main__':
    app.run(debug=True)