#Cashtag
======================================

## Big data pipeline for user sentiment analysis on US stock market
[www.hashtagcashtag.com](http://www.hashtagcashtag.com)

 #Cashtag is a big data pipeline to aggregate twitter data relevant to different stocks for New York Stock Exchange (NYSE) and NASDAQ stock market and provides an analytic framework to perform user sentiment analysis on different stocks and finding the correlation with the corresponding stock price.

## What #Cashtag Does
 #Cashtag allows user to easily check the top trending stocks @twitter at different time.

![#Cashtag Demo](Figures/tab1.png)


Users can look into the historical data and discover the top trending stocks and sentiment of twitter users on that stock at different hours of the day.
![#Cashtag Demo](Figures/tab2.png)

Users can also find the time series information about a stock - how many time the stock has been mentioned over time as well as corresponding sentiment. 
![#Cashtag Demo](Figures/tab3.png)

 #Cashtag also allows user to find the correlation betweent the number of mentions of a stock @twitter and the stocks price fluctuation over time.
![#Cashtag Demo](Figures/tab4.png)


# How #Cashtag Works
 #Cashtag pipeline is based on $\lambda$ architecture.The pipeline consists of an ingestion layer, batch layer, speed layer, serving layer and frontend. The pipeline diagram is shown below:

![Data Pipeline](Figures/pipeline.png) 

## Data Ingestion
 #Cashtag works by pulling twitter data and stock market data. #Cashtag uses the twitter streaming API. Due to the limitation of this API, the current version is limited to pulling data for about 250 stocks including NASDAQ 100, NYSE 100 and some other popular stocks from these to exchange. 

![Raw Twitter File](Figures/twitter.png)
 
 #Cashtag fetches stock price information from www.netfonds.no . The website provides sub-second interval level-1 stock prices delayed by an hour. #Cashtag fetches the stock information for each individual stock every 5 seconds. Some pre-processing is done on this stock information - e.g. adding a ticker information and a timestamp. 
 
 ![Raw Stock File](Figures/netfonds.png)
 
 
 public data from the United States Patent and Trademark Office (USPTO) 
and statistics from the Census Bureau, placing the files on the name node 
of a Amazon Web Services (AWS) cluster of Elastic Compute Cloud(EC2).  Specifically, a collection of Bash and Python 
scripts use Beautiful Soup to scrape the USPTO website to find the links to 
the relevant zip files.  The patent office releases new data weekly, which are
downloaded and updated with cron jobs.
  
The full fidelity zip files are immediately stored on the Hadoop Distributed File
 System (HDFS) for safe keeping via redundancy.  Some of the data is in simple 
Tab Separated Values (TSV) files, but the most recent patent information is
stored in highly nested XML files.  Semi-structured data like XML allows flexibile 
formatting, but can be tricky to parse on distributed systems, pulling out only
 the relevant information rather than the various tags (see sample below).

![Raw XML File](Figures/xml-multi.png)
 
Unfortunately, the USPTO lists each patent on several different lines, which vary
greatly from patent to patent depending on the number of authors, organizations, etc.  This is imcompatible with most
 technologies in the Hadoop ecosystem, which typically require a single record 
on a single line.  For this reason, the XML files are cleaned and parsed using 
Bash and Python scripts before further processing within the Hadoop ecosystem.

![Worked Node Parallelization](Figures/parallel.png)
  
To circumvent this bottleneck, I have wrote a meta-programming script that splits this workload 
onto the worker nodes.  This is accomplished by finding the ip addresses of the 
worker nodes using the *dfsadmin* command, then the *scp*, *ssh*, and *tmux* commands to distribute and run the 
relevant scripts.  This work around achieves scalability, but lacks the true fault
 tolerance of the Hadoop ecosystem.  

![Single-Line Record JSON](Figures/single-json.png)

## Batch Processing

After this staging, the patents are converted into single-line JSON records for further cleaning with Hive.  
JSON still has the flexibility of semi-structured data, but without the added size 
from the closing tags of XML.  An example of the JSON data, with some of the 
relevant information highlighted, is shown below with pretty-print formatting in both the 
original XML and the JSON formats.

![JSON Records](Figures/json.png)

After processing and staging on the Linux File System of the nodes, the JSON 
records are placed on the HDFS, partitioned by the year of the patent.  The files 
are then loaded into Hive tables using the open source serialization/deserialization (SerDe) 
tools developed by [Roberto Congiu](https://github.com/rcongiu/Hive-JSON-Serde).  My Hive 
schema is shown below with the same nested structure as the JSON files.

![Open Source Serialization Tool](Figures/serde.png)

This SerDe is perfect for my data, which is highly nested and has a very dynamic schema over the various 
years.  Unlike some of the native Hive tools, this SerDe allows me to ignore the 
superfluous information, and just extract what's needed.  More so, by taking full 
advantage of Hive, nearly two decades of data (around 50GB) can be calculated in a batch process of only 20 minutes...very cool!  
After this, the data is in the nice tabular form.

![Tabular Data after Hive Cleansing](Figures/tabular.png)

## Real-Time Queries

After this the data is aggregated over different time periods and placed into HBase in a highly denormalized form, facilitating
 real-time queries. This is done using short-fat tables with many, many columns that are automatically generated from scripts. 
By using multiple tables aggregated over different time granularities, the queries are easily sub-second for the whole data set.

![HBase Schemas](Figures/hbase-schema.png) 

For the front-end, I used the Thrift protocol via HappyBase with the Python Flask 
 library to setup a RESTful API.  This API is then queried for a nice visual UI, I modified 
the Java Script templates from Highmaps.  Feel free to play around and discover where innovation lives at 
[www.patentlyinnovative.net](http://www.patentlyinnovative.net).



