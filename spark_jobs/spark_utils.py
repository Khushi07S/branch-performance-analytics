import os
from pyspark.sql import SparkSession
from spark_jobs.config import (
    SPARK_HOME, HADOOP_HOME, VENV_PYTHON_PATH,
    SPARK_APP_NAME, SPARK_MASTER, SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY, SPARK_SQL_LEGACY_TIME_POLICY,
    SPARK_DRIVER_HOST, SPARK_DRIVER_BIND_ADDRESS
)

def get_spark_session():
    os.environ['SPARK_HOME'] = SPARK_HOME
    os.environ['HADOOP_HOME'] = HADOOP_HOME
    os.environ['PATH'] = os.environ['PATH'] + os.pathsep + \
                         os.environ['SPARK_HOME'] + os.pathsep + \
                         os.environ['HADOOP_HOME'] + r'\bin'
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY) \
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
        .config("spark.sql.legacy.timeParserPolicy", SPARK_SQL_LEGACY_TIME_POLICY) \
        .config("spark.driver.host", SPARK_DRIVER_HOST) \
        .config("spark.driver.bindAddress", SPARK_DRIVER_BIND_ADDRESS) \
        .config("spark.pyspark.python", VENV_PYTHON_PATH) \
        .config("spark.python.worker.timeout", "600") \
        .getOrCreate()
    print("Spark Session created successfully!")
    return spark