"""
Robust KPI ingestion script.

- Preferred path: PySpark -> write staging table via JDBC -> upsert via psycopg2
- Fallback path: pandas + psycopg2 (fast for dev/smaller data)
- Windows-safe Spark settings (forces driver host/bind to 127.0.0.1)

Improvements in this version:
- Normalizes column names (lowercase) for both Spark and pandas paths
- Clearer logging and retries for common column-name variants
- Keeps the automatic Spark -> staging -> psycopg2 upsert pattern
"""

import os
import sys
import traceback
from pathlib import Path
import shutil

# -----------------------
# CONFIG (adjust via env or edit here)
# -----------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Parquet input path (folder or file)
PROCESSED_DATA_PATH = os.environ.get('PROCESSED_DATA_PATH', str(PROJECT_ROOT / 'data' / 'processed'))

# JDBC / Postgres
PG_HOST = os.environ.get('DB_HOST', 'localhost')
PG_PORT = os.environ.get('DB_PORT', '5432')
PG_DBNAME = os.environ.get('DB_NAME', 'branch_analytics_db')
PG_USER = os.environ.get('DB_USER', 'branch_analytics_user')
PG_PASSWORD = os.environ.get('DB_PASS', 'postgres')
PG_TABLE = os.environ.get('PG_TABLE', 'branch_kpis_new')
PG_STAGE_TABLE = os.environ.get('PG_STAGE_TABLE', 'branch_kpis_stage')

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DBNAME}"
# Absolute JDBC jar path - set JDBC_DRIVER_PATH env var or place jar in backend/drivers/
JDBC_DRIVER_JAR = os.environ.get('JDBC_DRIVER_PATH') or str(PROJECT_ROOT / 'drivers' / 'postgresql-42.7.7.jar')

# PySpark python executable
PYSPARK_PYTHON = os.environ.get('PYSPARK_PYTHON', sys.executable)

# Required logical columns (lowercased)
REQUIRED_COLS = {
    'branches',
    'year',
    'quarter',
    'totalaggregatedeposits',
    'totalaggregatecredit',
    'totalcasa'
}

# -----------------------
# Helpers
# -----------------------
def can_run_spark():
    """Return True if 'java' and 'spark-submit' are available (or SPARK_HOME points to spark)."""
    java_exec = shutil.which("java")
    spark_submit = shutil.which("spark-submit")
    spark_home = os.environ.get('SPARK_HOME')
    if not spark_submit and spark_home:
        candidate = os.path.join(spark_home, 'bin', 'spark-submit')
        if os.name == 'nt':
            if os.path.exists(candidate + '.cmd'):
                candidate = candidate + '.cmd'
            elif os.path.exists(candidate + '.exe'):
                candidate = candidate + '.exe'
        if os.path.exists(candidate):
            spark_submit = candidate
    print(f"Detection: java found: {bool(java_exec)}  spark-submit found: {bool(spark_submit)}")
    return bool(java_exec) and bool(spark_submit)


def _log_cols(cols):
    try:
        print("Columns:", list(cols))
    except Exception:
        pass

# -----------------------
# Spark ingestion path
# -----------------------
def spark_ingest():
    try:
        from pyspark.sql import SparkSession
    except Exception as e:
        print("PySpark not importable:", e)
        return False

    # ensure driver uses loopback to avoid hostname RPC issues on Windows
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    os.environ['PYSPARK_PYTHON'] = PYSPARK_PYTHON
    os.environ['PYSPARK_DRIVER_PYTHON'] = PYSPARK_PYTHON

    # absolute JDBC jar path check
    if not os.path.exists(JDBC_DRIVER_JAR):
        print(f"JDBC driver not found at {JDBC_DRIVER_JAR}. Spark path will still attempt, but JDBC writes may fail.")
    print("Starting Spark session (local[*])...")

    try:
        spark = SparkSession.builder \
            .appName("BranchKPIsIngest") \
            .master("local[*]") \
            .config("spark.jars", JDBC_DRIVER_JAR) \
            .config("spark.driver.extraClassPath", JDBC_DRIVER_JAR) \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.network.timeout", "800s") \
            .config("spark.rpc.netty.dispatcher.numThreads", "4") \
            .getOrCreate()
    except Exception as e:
        print("Failed to create SparkSession:", e)
        traceback.print_exc()
        return False

    try:
        print(f"Reading Parquet data from: {PROCESSED_DATA_PATH}")
        df = spark.read.parquet(PROCESSED_DATA_PATH)
        print("Raw Parquet schema:")
        df.printSchema()
        df.show(3, truncate=False)

        # --- NORMALIZE COLUMN NAMES (Spark DataFrame) ---
        new_names = [c.lower() for c in df.columns]
        df = df.toDF(*new_names)
        print("Normalized Parquet schema (lowercase columns):")
        df.printSchema()

        # Log available columns
        _log_cols(df.columns)

        # Verify required columns
        missing = REQUIRED_COLS - set(df.columns)
        if missing:
            print(f"ERROR: Missing required columns in Parquet: {missing}")
            return False

        print(f"Writing DataFrame to JDBC staging table: {PG_STAGE_TABLE}")
        df.write.format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", PG_STAGE_TABLE) \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        print("Staging table written. Now performing upsert into production using psycopg2...")
        upsert_using_psycopg2()
        print("Upsert complete.")
        return True

    except Exception as e:
        print("Exception during Spark ingestion:", e)
        traceback.print_exc()
        return False
    finally:
        try:
            spark.stop()
            print("Spark session stopped.")
        except Exception:
            pass

# -----------------------
# Upsert function (psycopg2)
# -----------------------
def upsert_using_psycopg2():
    try:
        import psycopg2
    except Exception as e:
        print("psycopg2 not available:", e)
        raise

    conn = None
    try:
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD)
        cur = conn.cursor()
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {PG_TABLE} (
            branches TEXT NOT NULL,
            year INTEGER NOT NULL DEFAULT 0,
            quarter INTEGER NOT NULL DEFAULT 0,
            totalaggregatedeposits BIGINT DEFAULT 0,
            totalaggregatecredit BIGINT DEFAULT 0,
            totalcasa BIGINT DEFAULT 0,
            region TEXT,
            state TEXT,
            PRIMARY KEY (branches, year, quarter)
        );
        """
        cur.execute(create_table_sql)
        conn.commit()

        upsert_sql = f"""
        INSERT INTO {PG_TABLE} (branches, year, quarter, totalaggregatedeposits, totalaggregatecredit, totalcasa, region, state)
        SELECT branches, year, quarter, totalaggregatedeposits, totalaggregatecredit, totalcasa, region, state FROM {PG_STAGE_TABLE}
        ON CONFLICT (branches, year, quarter) DO UPDATE
          SET totalaggregatedeposits = EXCLUDED.totalaggregatedeposits,
              totalaggregatecredit = EXCLUDED.totalaggregatecredit,
              totalcasa = EXCLUDED.totalcasa,
              region = EXCLUDED.region,
              state = EXCLUDED.state;
        """
        cur.execute(upsert_sql)
        conn.commit()
        cur.close()
    except Exception as e:
        if conn:
            conn.rollback()
        print("Upsert via psycopg2 failed:", e)
        traceback.print_exc()
        raise
    finally:
        if conn:
            conn.close()

# -----------------------
# Pandas fallback ingestion
# -----------------------
def pandas_fallback_ingest():
    print("Starting pandas fallback ingestion (pyarrow + psycopg2).")
    try:
        import pyarrow.parquet as pq
        import pandas as pd
    except Exception as e:
        print("pyarrow/pandas not available:", e)
        return False

    try:
        print(f"Reading parquet from: {PROCESSED_DATA_PATH}")
        table = pq.read_table(PROCESSED_DATA_PATH)
        df = table.to_pandas()

        # --- NORMALIZE COLUMN NAMES (pandas DataFrame) ---
        df.columns = [c.lower() for c in df.columns]
        print("Pandas DataFrame columns (lowercased):", df.columns.tolist())

        required_cols = REQUIRED_COLS
        missing = required_cols - set(df.columns)
        if missing:
            print(f"Missing columns in parquet: {missing}")
            return False

        import psycopg2
        from psycopg2.extras import execute_values

        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME, user=PG_USER, password=PG_PASSWORD)
        cur = conn.cursor()

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {PG_TABLE} (
            branches TEXT NOT NULL,
            year INTEGER NOT NULL DEFAULT 0,
            quarter INTEGER NOT NULL DEFAULT 0,
            totalaggregatedeposits BIGINT DEFAULT 0,
            totalaggregatecredit BIGINT DEFAULT 0,
            totalcasa BIGINT DEFAULT 0,
            region TEXT,
            state TEXT,
            PRIMARY KEY (branches, year, quarter)
        );
        """)
        conn.commit()

        rows = df[['branches','year','quarter','totalaggregatedeposits','totalaggregatecredit','totalcasa','region','state']].fillna(None).values.tolist()
        insert_sql = f"""
        INSERT INTO {PG_TABLE} (branches, year, quarter, totalaggregatedeposits, totalaggregatecredit, totalcasa, region, state)
        VALUES %s
        ON CONFLICT (branches, year, quarter) DO UPDATE SET
          totalaggregatedeposits = EXCLUDED.totalaggregatedeposits,
          totalaggregatecredit = EXCLUDED.totalaggregatecredit,
          totalcasa = EXCLUDED.totalcasa,
          region = EXCLUDED.region,
          state = EXCLUDED.state
        ;
        """
        execute_values(cur, insert_sql, rows, page_size=1000)
        conn.commit()
        cur.close()
        conn.close()
        print("Pandas fallback ingestion complete.")
        return True
    except Exception as e:
        print("Pandas fallback error:", e)
        traceback.print_exc()
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        return False

# -----------------------
# Main runner with auto-detect
# -----------------------
if __name__ == "__main__":
    # Prefer Spark if environment supports it
    if not can_run_spark():
        print("Spark not available on PATH or SPARK_HOME. Attempting pandas fallback.")
        ok = pandas_fallback_ingest()
        if not ok:
            print("Both Spark and pandas ingestion failed. Fix environment or parquet path.")
            sys.exit(2)
        print("Ingestion completed via pandas fallback.")
        sys.exit(0)

    # Attempt Spark ingestion
    ok = spark_ingest()
    if not ok:
        print("Spark ingestion failed — attempting pandas fallback.")
        ok2 = pandas_fallback_ingest()
        if not ok2:
            print("Both ingestion methods failed.")
            sys.exit(2)
    print("Ingestion finished successfully.")