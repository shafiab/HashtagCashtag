# -*- coding: utf-8 -*-
"""
Created on Mon Feb  2 05:21:31 2015
this script submit stockProducer job for 
each individual stocks
@author: shafiab
"""
import os.path
import cashtagSet



cmd = 'nohup python /home/ubuntu/stockProducer/stockProducer.py '
#cmd = 'nohup python /home/ubuntu/stockProducer/stockProducerOneShot.py ' 
suffix = ' &'

def submitJob(stock, exchange):
    logFile = 'log'+stock+'.txt'
    cmdTotal = cmd+"'"+stock+"'"+' '+"'"+exchange+"'"+' > '+logFile+suffix
    print cmdTotal
    os.system(cmdTotal)


def doWork():
    exchange = 'O'
    stocks = cashtagSet.cashtagSet('NASDAQ100')
    for stock in stocks:
        ticker = stock.split('$')[1]
        submitJob(ticker, exchange)

    exchange = 'N'
    stocks = cashtagSet.cashtagSet('NYSE100')
    for stock in stocks:
        ticker = stock.split('$')[1]
        submitJob(ticker, exchange)

    exchange = 'N'
    stocks = cashtagSet.cashtagSet('COMPANIES')
    for stock in stocks:
        ticker = stock.split('$')[1]
        submitJob(ticker, exchange)


if __name__ == '__main__':
    doWork()
