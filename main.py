from silver import historical_summ



def gold_weekly():
    df_gold = historical_summ()
    df_gold.printSchema()

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
    df_gold.printSchema()

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




# gold_monthly()