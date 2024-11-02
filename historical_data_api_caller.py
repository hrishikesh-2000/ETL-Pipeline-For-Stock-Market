import requests
import json
from pyspark.sql import *

spark = SparkSession.builder.appName("Historical Data Caller").getOrCreate()


# Historical candle data to retreive OHLC (Open, High, Low, Close) about give instrument


url = "https://api.upstox.com/v2/historical-candle/:instrument_key/:interval/:to_date/:from_date"

payload={}
headers = {
  'Accept': 'application/json'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)

## testing in intial dev