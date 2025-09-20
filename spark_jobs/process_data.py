import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# --- 1. Initialize Spark Session ---
os.environ['SPARK_HOME'] = r'E:\Downloads\spark\spark-4.0.1-bin-hadoop3' 
os.environ['HADOOP_HOME'] = r'E:\Downloads\spark\spark-4.0.1-bin-hadoop3\hadoop'
os.environ['PATH'] = os.environ['PATH'] + ';' + os.environ['SPARK_HOME'] + r'\bin;' + os.environ['HADOOP_HOME'] + r'\bin'

spark = SparkSession.builder \
    .appName("BranchPerformanceAnalytics") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

print("Spark Session created successfully!")

# --- 2. Define Data Paths ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, os.pardir))
raw_data_path = os.path.join(project_root, "data", "raw", "DATASET- BANK BRANCH DATA", "BRANCH DATA - latest.csv")
processed_data_path = os.path.join(project_root, "data", "processed", "branch_kpis.parquet")
os.makedirs(os.path.dirname(processed_data_path), exist_ok=True)

# --- 3. Ingest Data ---
try:
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(raw_data_path)
    print(f"\nSuccessfully loaded data from: {raw_data_path}")
    print("Schema:")
    df.printSchema()
    print("\nFirst 5 rows:")
    df.show(5)

    branch_id_col = "Branches" 
    current_dep_count = "CurrentDep" 

    if "Branches" in df.columns and "CurrentDep" in df.columns:
        print("\nCalculating Total Current Deposits per Branch...")
        total_deposits_per_branch = df.groupBy(col("Branches")) \
                                    .agg(spark_sum(col("CurrentDep")).alias("TotalCurrentDeposits")) \
                                    .orderBy(col("TotalCurrentDeposits").desc())

        print("\nTotal Current Deposits count per Branch:")
        total_deposits_per_branch.show()

        # --- 5. Save Processed Data ---
        print(f"\nSaving processed data to: {processed_data_path}")
        total_deposits_per_branch.write.mode("overwrite").parquet(processed_data_path)
        print("Processed data saved as Parquet.")

    else:
        print("\nWarning: Could not find 'Branches' or 'CurrentDep' columns. Please inspect your CSV schema and update the script.")
        print("Available columns:", df.columns)


except Exception as e:
    print(f"An error occurred: {e}")
    print(f"Make sure the CSV file is at: {raw_data_path}")

finally:
    # --- 6. Stop Spark Session ---
    spark.stop()
    print("\nSpark Session stopped.")