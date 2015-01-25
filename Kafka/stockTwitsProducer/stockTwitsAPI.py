from kafka import *
from cashtagSetNew import cashtagSetNew
import json
import requests
import datetime
import threading

# kafka setup
mykafka = KafkaClient("localhost:9092")
producer = SimpleProducer(mykafka)
topicName = "stockTwitsStream"

def stream_symbol(symbol):        
    url = "https://api.stocktwits.com/api/2/streams/symbol/" + str(symbol) + ".json"
    print url
    try:
        content = requests.get(url).text
    except Exception as e:
        print e
	retVal = []
	return retVal
    
    return json.loads(content)

def getTweets(stocks):
    #result = []
    for stock in stocks:
	res = stream_symbol(stock)
	if len(res)==0: #error in fetching
		continue
	else: # fetching successful	
		if res['response']['status']==200:
			producer.send_messages(topicName, json.dumps(res['messages']))
			#result+= res['messages']
		else:
			continue
   # return result


def fetchAndSend():
   # result = []
    
    stocks = cashtagSetNew("NYSE100")
    getTweets(stocks)
    
    stocks = cashtagSetNew("NASDAQ100")
    getTweets(stocks)
    
    stocks = cashtagSetNew("COMPANIES")
    getTweets(stocks)
    #producer.send_messages(topicName, json.dumps(result))


def doWork():
    print datetime.datetime.now()

    fetchAndSend()
    threading.Timer(3600, doWork).start()

if __name__ == "__main__":
    doWork()
    



