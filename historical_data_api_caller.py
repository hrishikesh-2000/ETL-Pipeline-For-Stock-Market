import requests
import json
from pyspark.sql import *

spark = SparkSession.builder.appName("Historical Data Caller").getOrCreate()


# Historical candle data to retreive OHLC (Open, High, Low, Close) about give instrument