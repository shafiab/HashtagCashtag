Patently Innovative
======================================

## Finding where innovation lives!
[www.patentlyinnovative.net](http://www.patentlyinnovative.net)

Patently Innovative is a tool to track innovation in different states 
by collecting, cleaning, and aggregating patent information from the 
United States Patent and Trademark Office (USPTO) using technologies from the
 Hadoop ecosystem.

# What Patently Innovative Does
Patently Innovative allows users to easily check and compare the patent 
production trends of various states.  The user interface, built with
Highmaps, allows the user to click on as many states as desired and
see the trends for patent production.  Patent production can be viewed 
as a raw total or per capita.  For example, you can easily compare the 
patent trends between California and Texas.

![Patently Innovative Demo](figures/demo.png)

Patent information can be displayed at different granularities in time, 
either monthly or yearly.

![Patently Innovative Demo](figures/demo-year.png)

Trends can also be filtered by different patent classifications, allowing 
easy comparison of patents in a single sector, such as technology patents.

![Patently Innovative Demo](figures/demo-tech.png)

# How Patently Innovative Works
Patently Innovative uses a technology stack consisting of Beautiful Soup web-scraping, 
Bash and Python
 scripts, Hadoop Streaming, JSON Serialization with Hive, HBase, Flask, and Highcharts Java Script.

![Data Pipeline](figures/pipeline.png) 

## Data Ingestion
Patently Innovative works by pulling public data from the United States Patent and Trademark Office (USPTO) 
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

![Raw XML File](figures/xml-multi.png)
 
Unfortunately, the USPTO lists each patent on several different lines, which vary
greatly from patent to patent depending on the number of authors, organizations, etc.  This is imcompatible with most
 technologies in the Hadoop ecosystem, which typically require a single record 
on a single line.  For this reason, the XML files are cleaned and parsed using 
Bash and Python scripts before further processing within the Hadoop ecosystem.

![Worked Node Parallelization](figures/parallel.png)
  
To circumvent this bottleneck, I have wrote a meta-programming script that splits this workload 
onto the worker nodes.  This is accomplished by finding the ip addresses of the 
worker nodes using the *dfsadmin* command, then the *scp*, *ssh*, and *tmux* commands to distribute and run the 
relevant scripts.  This work around achieves scalability, but lacks the true fault
 tolerance of the Hadoop ecosystem.  

![Single-Line Record JSON](figures/single-json.png)

## Batch Processing

After this staging, the patents are converted into single-line JSON records for further cleaning with Hive.  
JSON still has the flexibility of semi-structured data, but without the added size 
from the closing tags of XML.  An example of the JSON data, with some of the 
relevant information highlighted, is shown below with pretty-print formatting in both the 
original XML and the JSON formats.

![JSON Records](figures/json.png)

After processing and staging on the Linux File System of the nodes, the JSON 
records are placed on the HDFS, partitioned by the year of the patent.  The files 
are then loaded into Hive tables using the open source serialization/deserialization (SerDe) 
tools developed by [Roberto Congiu](https://github.com/rcongiu/Hive-JSON-Serde).  My Hive 
schema is shown below with the same nested structure as the JSON files.

![Open Source Serialization Tool](figures/serde.png)

This SerDe is perfect for my data, which is highly nested and has a very dynamic schema over the various 
years.  Unlike some of the native Hive tools, this SerDe allows me to ignore the 
superfluous information, and just extract what's needed.  More so, by taking full 
advantage of Hive, nearly two decades of data (around 50GB) can be calculated in a batch process of only 20 minutes...very cool!  
After this, the data is in the nice tabular form.

![Tabular Data after Hive Cleansing](figures/tabular.png)

## Real-Time Queries

After this the data is aggregated over different time periods and placed into HBase in a highly denormalized form, facilitating
 real-time queries. This is done using short-fat tables with many, many columns that are automatically generated from scripts. 
By using multiple tables aggregated over different time granularities, the queries are easily sub-second for the whole data set.

![HBase Schemas](figures/hbase-schema.png) 

For the front-end, I used the Thrift protocol via HappyBase with the Python Flask 
 library to setup a RESTful API.  This API is then queried for a nice visual UI, I modified 
the Java Script templates from Highmaps.  Feel free to play around and discover where innovation lives at 
[www.patentlyinnovative.net](http://www.patentlyinnovative.net).



