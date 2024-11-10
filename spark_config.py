from pyspark.sql import *

def spark():

    spark = SparkSession\
        .builder\
        .appName("Stock Market ETL")\
        .enableHiveSupport()\
        .master("local[*]")\
        .getOrCreate()

    return spark




