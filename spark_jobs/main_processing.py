# spark_jobs/main_processing.py

import os
# NEW IMPORTS for validation
from pyspark.sql import functions as F
from spark_jobs.spark_utils import get_spark_session
from spark_jobs.data_ingestion import load_raw_data, generate_branch_location_data
from spark_jobs.data_transformation import add_location_data, clean_and_transform_dates
from spark_jobs.kpi_calculation import calculate_branch_kpis
from spark_jobs.config import PROCESSED_DATA_PATH, BANK_NAME
from spark_jobs.data_validator import ( # NEW IMPORTS
    check_null_values, check_duplicates, check_referential_integrity,
    detect_outliers_zscore, check_business_logic_kpis
)

def run_data_pipeline():
    spark = None
    try:
        # 1. Initialize Spark Session
        spark = get_spark_session()
        print("\n--- Spark Session Initialized ---")

        # 2. Generate and Load Data
        print("\n--- Data Ingestion ---")
        raw_df = load_raw_data(spark)
        print("Raw Data Schema:")
        raw_df.printSchema()
        print("Raw Data First 5 rows:")
        raw_df.show(5)

        # --- Data Validation: Initial Raw Data ---
        check_null_values(raw_df, "Raw Data DataFrame")
        # Assuming combination of Branch and DATE should be unique for raw daily snapshots
        check_duplicates(raw_df, "Raw Data DataFrame", subset_cols=["Branches", "DATE"])

        branch_locations_df = generate_branch_location_data(spark)
        print("\nBranch Location Data First 5 rows:")
        branch_locations_df.show(5)

        # --- Data Validation: Location Data ---
        check_null_values(branch_locations_df, "Branch Locations DataFrame")
        # Branches should be unique in the location data
        check_duplicates(branch_locations_df, "Branch Locations DataFrame", subset_cols=["Branches"])


        # 3. Transform Data
        print("\n--- Data Transformation ---")
        df_with_location = add_location_data(raw_df, branch_locations_df)
        print("Data with Location Info First 5 rows:")
        df_with_location.show(5)

        # --- Data Validation: Joined Data (after add_location_data) ---
        check_null_values(df_with_location, "Data with Location DataFrame")
        # Check referential integrity for branches
        check_referential_integrity(
            parent_df=branch_locations_df, parent_key="Branches",
            child_df=df_with_location, child_key="Branches",
            parent_df_name="Branch Locations DataFrame", child_df_name="Data with Location DataFrame"
        )


        df_cleaned_and_filtered = clean_and_transform_dates(df_with_location)
        print("Cleaned and Filtered Data Schema:")
        df_cleaned_and_filtered.printSchema()
        print("Cleaned and Filtered Data First 5 rows:")
        df_cleaned_and_filtered.show(5)

        # --- Data Validation: Cleaned & Filtered Data - Outlier Detection ---
        # Define numerical columns for outlier detection (adjust based on your actual data and business understanding)
        # Assuming these columns exist after clean_and_transform_dates
        numerical_cols_for_outliers = [
            "CurrentDep", "SavingsDep", "CASA", "TermDep", "AggregateDep",
            "RetailCredit", "Agriculture", "BusinessCredit", "AggregateCredit", "AggregateBusiness"
        ]
        detect_outliers_zscore(df_cleaned_and_filtered, "Cleaned and Filtered Data", numerical_cols_for_outliers)


        # 4. Calculate KPIs
        print(f"\n--- KPI Calculation for {BANK_NAME} ---")
        kpis_df = calculate_branch_kpis(df_cleaned_and_filtered)
        print(f"Calculated {kpis_df.count()} KPI records for {BANK_NAME}.")
        print("KPIs Data Schema:")
        kpis_df.printSchema()
        print("KPIs Data First 10 rows:")
        kpis_df.show(10)

        # --- Data Validation: Calculated KPIs ---
        check_null_values(kpis_df, "Calculated KPIs DataFrame")
        # KPIs should be unique per branch, year, and quarter
        check_duplicates(kpis_df, "Calculated KPIs DataFrame", subset_cols=["Branches", "Year", "Quarter"])
        check_business_logic_kpis(kpis_df, "Calculated KPIs DataFrame")

        # Numerical columns for outlier detection in KPIs (can be different from raw data)
        numerical_kpi_cols_for_outliers = [
            "TotalCurrentDeposits", "TotalSavingsDeposits", "TotalCASA", "TotalTermDeposits",
            "TotalAggregateDeposits", "TotalRetailCredit", "TotalAgricultureCredit",
            "TotalBusinessCredit", "TotalAggregateCredit", "TotalAggregateBusiness",
            "ActiveDays", "TotalTransactionsCount" # Add these if they are output by calculate_branch_kpis
        ]
        detect_outliers_zscore(kpis_df, "Calculated KPIs DataFrame (Aggregated)", numerical_kpi_cols_for_outliers)


        # 5. Save Processed Data
        print(f"\n--- Data Storage ---")
        os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
        kpis_df.write \
            .mode("overwrite") \
            .parquet(PROCESSED_DATA_PATH)
        print(f"Processed data for {BANK_NAME} saved as Parquet to: {PROCESSED_DATA_PATH}")

    except Exception as e:
        print(f"An error occurred during the pipeline execution: {e}")
    finally:
        if spark:
            spark.stop()
            print("Spark Session stopped.")

if __name__ == "__main__":
    run_data_pipeline()