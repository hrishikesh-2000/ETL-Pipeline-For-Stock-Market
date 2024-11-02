from pyspark.sql import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Instrument info").master("local[*]").getOrCreate()

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

df = spark.read.format("csv").option("header","True").option("inferSchema","True").load("NSE.csv")

df_new = spark.createDataFrame( data = [row for row in df.collect()])

df_new.filter(df.lot_size.isnotNull()).collect()