import os

# Environment variables for Spark and Hadoop
SPARK_HOME = r'E:\Downloads\spark\spark-4.0.1-bin-hadoop3' 
HADOOP_HOME = r'E:\Downloads\spark\spark-4.0.1-bin-hadoop3\hadoop'

# Define Data Paths 
current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, os.pardir))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "DATASET- BANK BRANCH DATA", "BRANCH DATA - latest.csv")
PROCESSED_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "processed")

# Spark Configuration 
SPARK_APP_NAME = "BranchPerformanceAnalytics"
SPARK_MASTER = "local[*]"
SPARK_DRIVER_MEMORY = "4g"
SPARK_EXECUTOR_MEMORY = "4g"
SPARK_SQL_LEGACY_TIME_POLICY = "LEGACY"
SPARK_DRIVER_HOST = "127.0.0.1"
SPARK_DRIVER_BIND_ADDRESS = "127.0.0.1"

# Python Configuration (Crucial for Windows Worker) 
VENV_PYTHON_PATH = r'E:\University\Project\branch-analytics-dashboard\branch-performance-analytics\.venv\Scripts\python.exe'

# Project Constants 
BANK_NAME = "BharatX Bank Corp."
NUM_BRANCHES = 100 
FILTER_QUARTERS_DAYS = 270 # Approx 9 months or last 3 quarters

# Data validation
OUTLIER_THRESHOLD_Z_SCORE = 3.0