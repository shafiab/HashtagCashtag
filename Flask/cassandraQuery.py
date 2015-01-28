
from flask import Flask, render_template, redirect, url_for, request, jsonify
from cassandra.cluster import Cluster
import time
import pandas as pd
import numpy as np
import time
import json

import time
import datetime

app = Flask(__name__)
cluster = Cluster(['54.67.105.220'])

session = cluster.connect('stockdata')
rows = session.execute("select * from daystock where ticker='FB'")
#println rows
data = []
for r in rows:    
    a = [str(r.year)+'-'+str(r.month)+'-'+str(r.day), r.open, r.high,  r.low, r.close]
    data.append(a)

print data
#rows = session.execute("SELECT * FROM fantasy.players WHERE name = '" + player + "'")
#session = cluster.connect('test')
