from pyspark.sql import *

def spark():
    spark = SparkSession\
        .builder\
        .appName("Instrument info")\
        .master("local[*]")\
        .getOrCreate()

    return spark




