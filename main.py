from silver import historical_summ
from spark_config import spark


from pyspark.sql.functions import *
from pyspark.sql import *

import os

spark = spark()

def gold_weekly():
    df_gold = historical_summ()
    # df_gold.printSchema()

    df = df_gold.withColumn("week_of_year", weekofyear(df_gold.timestamp))

    df_week = df.groupBy("ISIN_code", 'company_name', 'industry', "week_of_year").agg(
        first(df.open).alias("weekly_open"),
        max(df.high).alias("weekly_high"),
        min(df.low).alias("weekly_low"),
        last(df.close).alias("weekly_close"),
        round(avg(df.close),2).alias("weekly_avg_price")

    ).sort(df.company_name, df.week_of_year)\
    .withColumn("weekly_price_range", round(col("weekly_high") - col("weekly_low"),2))\
    .withColumn("weekly_return",round((col("weekly_close") - col("weekly_open"))/col("weekly_open") * 100, 2))

    window = Window.partitionBy("company_name").orderBy("week_of_year").rowsBetween(-2,Window.currentRow)

    df_weekly = df_week.withColumn("sma_3_week", round(avg(col("weekly_close")).over(window),2))

    return df_weekly


def gold_monthly():
    df_gold = historical_summ()

    df = df_gold.withColumn("month", month(df_gold.timestamp))

    df_month = df.groupBy("ISIN_code", 'company_name', 'industry', "month").agg(
        first(df.open).alias("monthly_open"),
        max(df.high).alias("monthly_high"),
        min(df.low).alias("monthly_low"),
        last(df.close).alias("monthly_close"),
        round(avg(df.close), 2).alias("monthly_avg_price")

    ).sort(df.company_name, df.month) \
        .withColumn("monthly_price_range", round(col("monthly_high") - col("monthly_low"), 2)) \
        .withColumn("monthly_return", round((col("monthly_close") - col("monthly_open")) / col("monthly_open") * 100, 2))


    window = Window.partitionBy("company_name").orderBy("month").rowsBetween(-2, Window.currentRow)

    df_monthly = df_month.withColumn("sma_2_month", round(avg(col("monthly_close")).over(window), 2))

    return df_monthly


def daily_indicator():

    df_delta = historical_summ()

    # Calculating the Relative Strength Index based on delta_close column.
    # Adding column upward_change and downward_change

    df = df_delta.withColumn("upward_change", when(col('delta_close') > 0, col("delta_close")).otherwise(0))\
                 .withColumn("downward_change", when(col('delta_close') < 0, -(col("delta_close"))).otherwise(0))

    window_rsi = Window.partitionBy("company_name").orderBy("timestamp").rowsBetween(-13, Window.currentRow)


    df_rsi = df.withColumn("rsi", round(100 - (100/(1 + (avg(col("upward_change")).over(window_rsi)/avg(col("downward_change")).over(window_rsi)))),2))

    df_rsi = df_rsi.drop("instrument_key", "symbol", "open_interest", "last_close","upward_change","downward_change")
    return df_rsi


def main():

    # Creating dataframes that can be further called to create views
    df_historical_summ = historical_summ()
    weekly_summ = gold_weekly()
    monthly_summ = gold_monthly()
    rsi_14 = daily_indicator()

    # Creating SQL views out of silver data
    df_historical_summ.createOrReplaceGlobalTempView("histoical_summ")
    weekly_summ.createOrReplaceGlobalTempView("weekly_summ")
    monthly_summ.createOrReplaceGlobalTempView("monthly_summ")
    rsi_14.createOrReplaceGlobalTempView("rsi_14")

    # Creating output directory
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Writing dataframes to csv files
    df_historical_summ.toPandas().to_csv("output/histoical_summ.csv")
    weekly_summ.toPandas().to_csv("output/weekly_summ.csv")
    monthly_summ.toPandas().to_csv("output/monthly_summ.csv")
    rsi_14.toPandas().to_csv("output/rsi_14.csv")

    # Testing with sql statments
    # spark.sql("select * from global_temp.instrument").show()
    # spark.sql("select * from global_temp.nifty_5").show()
    # spark.sql("select * from global_temp.histoical_summ").show()
    # spark.sql("select * from global_temp.weekly_summ").show()
    # spark.sql("select * from global_temp.monthly_summ").show()
    # spark.sql("select * from global_temp.rsi_14").show()


if __name__ = "__main__":
    main()