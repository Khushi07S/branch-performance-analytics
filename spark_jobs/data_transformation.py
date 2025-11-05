from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, lit, date_sub, expr
from spark_jobs.config import FILTER_QUARTERS_DAYS

def add_location_data(df: DataFrame, branch_locations_df: DataFrame) -> DataFrame:

    joined_df = df.join(branch_locations_df, on="Branches", how="left")
    print("Joined main DataFrame with branch location data.")
    joined_df.show(5)
    return joined_df

def clean_and_transform_dates(df: DataFrame) -> DataFrame:
    df = df.withColumn("TransactionDate", to_date(expr("concat('20', substring(DATE, 7, 2), '-', substring(DATE, 4, 2), '-', substring(DATE, 1, 2))"), "yyyy-MM-dd")) \
       .drop("DATE")
    print("Converted 'DATE' to 'TransactionDate' and dropped original:")
    df.printSchema()
    df.show(5)

    max_data_date = df.agg({"TransactionDate": "max"}).collect()[0][0]
    print(f"Latest Transaction Date in data: {max_data_date}")

    filter_start_date = df.select(date_sub(lit(max_data_date), FILTER_QUARTERS_DAYS).alias("CalculatedStartDate")).collect()[0][0]
    print(f"Filtering data from: {filter_start_date} to {max_data_date}")

    df_filtered = df.filter((col("TransactionDate") >= lit(filter_start_date))
                             & (col("TransactionDate") <= lit(max_data_date)))

    print(f"Data filtered for last approximately three quarters. Row count before filter: {df.count()}, after filter: {df_filtered.count()}")
    df_filtered.show(5)
    return df_filtered