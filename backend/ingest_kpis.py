import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

# Environment variables for Spark and Hadoop
SPARK_HOME = r'E:\Downloads\spark\spark-4.0.1-bin-hadoop3' 
HADOOP_HOME = r'E:\Downloads\spark\spark-4.0.1-bin-hadoop3\hadoop'
VENV_PYTHON_PATH = r'E:\University\Project\branch-analytics-dashboard\branch-performance-analytics\.venv\Scripts\python.exe'

#os.environ['SPARK_HOME'] = SPARK_HOME
#os.environ['HADOOP_HOME'] = HADOOP_HOME
#os.environ['PATH'] = os.environ.get('PATH', '') + os.pathsep + os.path.join(SPARK_HOME, 'bin') + os.pathsep + os.path.join(HADOOP_HOME, 'bin')
#os.environ['PYSPARK_PYTHON'] = VENV_PYTHON_PATH

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "processed")
JDBC_DRIVER_PATH = os.path.join(PROJECT_ROOT, "drivers", "postgresql-42.7.7.jar") 

PG_DBNAME = "branch_analytics_db"
PG_USER = "branch_analytics_user"
PG_PASSWORD = "postgres123" 
PG_HOST = "localhost"
PG_PORT = "5432"
PG_TABLE = "branch_kpis"
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DBNAME}"
JDBC_URL_WITH_TZ = f"{JDBC_URL}?currentSchema=public&stringtype=unspecified&allowEncodingChanges=true"

def ingest_kpis_to_postgres():
    print("--- Starting KPI Ingestion to PostgreSQL ---")

    # Initialize SparkSession with JDBC driver
    try:
        spark = SparkSession.builder \
            .appName("KPIPostgresIngestion") \
            .config("spark.jars", JDBC_DRIVER_PATH) \
            .config("spark.driver.extraClassPath", JDBC_DRIVER_PATH) \
            .getOrCreate()
        print("Spark Session created with JDBC driver.")
    except Exception as e:
        print(f"Error initializing SparkSession: {e}")
        return

    try:
        # Read the processed Parquet data
        print(f"Reading Parquet data from: {PROCESSED_DATA_PATH}")
        kpis_df = spark.read.parquet(PROCESSED_DATA_PATH)
        print("Successfully loaded KPI data from Parquet.")
        kpis_df.printSchema()
        kpis_df.show(5, truncate=False)

        # Write the DataFrame to PostgreSQL
        print(f"Writing data to PostgreSQL table '{PG_TABLE}' in database '{PG_DBNAME}'...")

        kpis_df.write \
            .format("jdbc") \
            .option("url", JDBC_URL_WITH_TZ) \
            .option("dbtable", PG_TABLE) \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .option("stringtype", "unspecified") \
            .mode("overwrite") \
            .save()
        print("Successfully ingested KPI data into PostgreSQL.")

    except AnalysisException as ae:
        print(f"Spark SQL Analysis Error: {ae}")
        print("Please ensure the Parquet files exist and are accessible at the specified path.")
    except Exception as e:
        print(f"An unexpected error occurred during ingestion: {e}")
    finally:
        spark.stop()
        print("Spark Session stopped.")
        print("--- KPI Ingestion Finished ---")

if __name__ == "__main__":
    ingest_kpis_to_postgres()