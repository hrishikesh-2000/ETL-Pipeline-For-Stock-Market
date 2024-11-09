import requests
import json
from urllib.parse import quote
from pyspark.sql import *
from pyspark.sql.types import *
from spark_config import


# Initiate the spark object
spark = spark()


# Historical candle data to retreive OHLC (Open, High, Low, Close) about give instrument
instrument_key = "NSE_INDEX|Nifty 50"
encoded_instrument_key = quote(instrument_key)
interval = "1minute"
to_date = "2024-11-02"
from_date = "2024-01-01"

url = f"https://api.upstox.com/v2/historical-candle/{encoded_instrument_key}/{interval}/{to_date}/{from_date}"


params= {
  "instrument_key": instrument_key,
  "interval": interval,
  "to_date": to_date,
  "from_date": from_date
}


headers = {
  'Accept': 'application/json'
}


response = requests.request("GET", url, headers=headers, params = params)

response = response.json()

# print(json.dumps(response , indent= 2))



#Define the schema for dataframe
schema = StructType([

        StructField('timestamp', StringType()),
        StructField('open', StringType()),
        StructField('high', StringType()),
        StructField('low', StringType()),
        StructField('close', StringType()),
        StructField('volume', StringType()),
        StructField('open_interest', StringType()),
  ]
)

df = spark.createDataFrame(data = response['data']['candles'], schema = schema)

# Map the datatype according to our usecase
df_new = df.withColumn(
    'timestamp', df['timestamp'].cast(TimestampType()))\
    .withColumn('open', df['open'].cast(DoubleType()))\
    .withColumn('high', df['high'].cast(DoubleType()))\
    .withColumn('low', df['low'].cast(DoubleType()))\
    .withColumn('close', df['close'].cast(DoubleType()))\
    .withColumn('volume', df['volume'].cast(DoubleType()))\
    .withColumn('open_interest', df['open_interest'].cast(DoubleType()))


# df_new.printSchema()
