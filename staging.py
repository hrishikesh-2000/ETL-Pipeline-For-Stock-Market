from pyspark.sql import *
from pyspark.sql.types import *
from spark_config import spark
import requests
from pyspark.sql.functions import *
from urllib.parse import quote
import json


def instrument(spark, csv):

    schema = StructType(
        [
            StructField("instrument_key", StringType()),
            StructField("exchange_token", StringType()),
            StructField("tradingsymbol", StringType()),
            StructField("name", StringType()),
            StructField("last_price", StringType()),
            StructField("expiry", StringType()),
            StructField("strike", StringType()),
            StructField("tick_size", StringType()),
            StructField("lot_size", StringType()),
            StructField("instrument_type", StringType()),
            StructField("option_type", StringType()),
            StructField("exchange", StringType())

        ]
    )

    df = spark.read.format("csv").option("header","True").schema(schema).load(csv)

    df_new = spark.createDataFrame( data = [row for row in df.collect()])

    return df_new


# instrument = instrument(spark,"NSE.csv")
# instrument.printSchema()

def historical_data(spark, params):

    instrument_key = params['instrument_key']
    encoded_instrument_key = quote(instrument_key)
    interval = params['interval']
    to_date = params['to_date']
    from_date = params['from_date']

    headers = {
        'Accept': 'application/json'
    }

    url =  f"https://api.upstox.com/v2/historical-candle/{encoded_instrument_key}/{interval}/{to_date}/{from_date}"

    response = requests.request("GET", url = url, headers=headers)

    response = response.json()

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

    df = spark.createDataFrame(data=response['data']['candles'], schema=schema)

    # Map the datatype according to our usecase
    df_new = df.withColumn(
        'timestamp', df['timestamp'].cast(TimestampType())) \
        .withColumn('open', df['open'].cast(DoubleType())) \
        .withColumn('high', df['high'].cast(DoubleType())) \
        .withColumn('low', df['low'].cast(DoubleType())) \
        .withColumn('close', df['close'].cast(DoubleType())) \
        .withColumn('volume', df['volume'].cast(DoubleType())) \
        .withColumn('open_interest', df['open_interest'].cast(DoubleType()))

    return df_new


# historical_data = historical_data(spark, params)
# historical_data.printSchema()

def nifty_top(spark, csv):

    df = spark.read.format("csv").option("header","true").load(csv)
    df = df.withColumnsRenamed(
        {
            "Company Name":"company_name",
            "Industry":"industry",
            "Symbol":"symbol",
            "Series":"series",
            "ISIN Code":"ISIN_code"
        }
    )

    return df

# df_nifty_top_5 = nifty_top(spark,"ind_nifty50list.csv")

# df_nifty_top_5.printSchema()



