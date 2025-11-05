from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, countDistinct, year, quarter, lit
from spark_jobs.config import BANK_NAME

def calculate_branch_kpis(df_filtered: DataFrame) -> DataFrame:
    df_kpis = df_filtered.withColumn("Year", year(col("TransactionDate"))) \
                         .withColumn("Quarter", quarter(col("TransactionDate")))
    print(f"Calculating KPIs for {BANK_NAME} per Branch, Year, Quarter, State, City, Region...")

    branch_kpis_df = df_kpis.groupBy("Branches", "Year", "Quarter", "State", "City", "Region") \
                            .agg(
                                # Deposit-related KPIs
                                spark_sum(col("CurrentDep")).alias("TotalCurrentDeposits"),
                                spark_sum(col("SavingsDep")).alias("TotalSavingsDeposits"),
                                spark_sum(col("CASA")).alias("TotalCASA"),
                                spark_sum(col("TermDep")).alias("TotalTermDeposits"),
                                spark_sum("AggregateDep").alias("TotalAggregateDeposits"),

                                #Credit-related KPIs
                                spark_sum("RetailCredit").alias("TotalRetailCredit"),
                                spark_sum("Agriculture").alias("TotalAgricultureCredit"),
                                spark_sum("BusinessCredit").alias("TotalBusinessCredit"),
                                spark_sum("AggregateCredit").alias("TotalAggregateCredit"),
                                spark_sum("AggregateBusiness").alias("TotalAggregateBusiness"),

                                # Other useful KPIs
                                countDistinct("TransactionDate").alias("ActiveDays"),
                                spark_sum(lit(1)).alias("TotalTransactionsCount")
                            ) \
                            .orderBy("Branches","Year","Quarter","State","City","Region")
    print(f"Calculated {branch_kpis_df.count()} KPI records for {BANK_NAME}.")
    print("Branch KPIs Schema:")
    branch_kpis_df.printSchema()
    print("First 10 rows of Branch KPIs:")
    branch_kpis_df.show(10)
    return branch_kpis_df