from staging import *
from spark_config import spark

spark = spark()


df_instrument = instrument(spark, "NSE.csv")


df_nifty_top_50 = nifty_top(spark, "ind_nifty50list.csv")
df_nifty_top_5 = df_nifty_top_50.limit(5)
# df_nifty_top_5.printSchema()


# Adding column ISCE_code in df_instrument
df_instrument_2 = df_instrument.withColumns({
    "ISIN_code" : split(df_instrument.instrument_key, "[|]", 2)[1]
})

# Join df_instrument_2 to get top 5 Nifty 50 companies
df_joined = df_instrument_2.join(df_nifty_top_5, [ df_instrument_2.ISIN_code == df_nifty_top_5.ISIN_code])

# Get the column instrument_key and convert it to list
list_instrument_key = [row.instrument_key for row in df_joined.collect()]


# Defining empty list for dataframes to be appended
dfs = []

for key in list_instrument_key:
    params = {
        "instrument_key": key,
        "interval": "day",
        "to_date": "2024-11-02",
        "from_date": "2024-10-02"
    }

    df = historical_data(spark,params)

    # Adding new column for future identification
    df = df.withColumn("instrument_key",lit(key)) \
            .withColumn("ISIN_code", lit(key.split("|")[1]))

    # Reorder columns to place the new column at the beginning
    df = df.select("ISIN_code","instrument_key", *df.columns[:-2])

    dfs.append(df)

def dataframe_union(dfs):
    if dfs:
        df_first = dfs[0]
    for df in dfs[1:]:
        df_first = df_first.unionAll(df)

    return df_first

df_union = dataframe_union(dfs)

# df_union.toPandas().to_csv("nifty_top_5_history.csv")

df_historical_top_5 = df_union.join(
    df_nifty_top_50,
    on=[df_union.ISIN_code == df_nifty_top_50.ISIN_code],
    how="inner")\
    .select(
    df_union["ISIN_code"].alias("ISIN_code"),
    "instrument_key",
    "company_name",
    "industry",
    "symbol",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest"

)



