from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_jobs.config import OUTLIER_THRESHOLD_Z_SCORE

def check_null_values(df: DataFrame, df_name: str):
    print(f"\n--- Null Value Check for {df_name} ---")
    null_counts = df.select([F.sum(F.col(c).isNull().cast("integer")).alias(c) for c in df.columns])
    null_counts_df = null_counts.toPandas()

    has_nulls = False
    for col_name in null_counts_df.columns:
        count = null_counts_df[col_name][0]
        if count > 0:
            print(f"Column '{col_name}': {count} null values found.")
            has_nulls = True
    if not has_nulls:
        print("No null values found in any column.")
    print("-" * (25 + len(df_name)))
    return null_counts_df 

def check_duplicates(df: DataFrame, df_name: str, subset_cols=None):
    print(f"\n--- Duplicate Check for {df_name} ---")
    if subset_cols:
        duplicate_count = df.exceptAll(df.dropDuplicates(subset_cols)).count()
        if duplicate_count > 0:
            print(f"Found {duplicate_count} duplicate rows based on columns: {subset_cols}")
        else:
            print(f"No duplicate rows found based on columns: {subset_cols}")
    else:
        total_rows = df.count()
        distinct_rows = df.dropDuplicates().count()
        duplicate_count = total_rows - distinct_rows
        if duplicate_count > 0:
            print(f"Found {duplicate_count} duplicate rows across all columns.")
        else:
            print("No duplicate rows found across all columns.")
    print("-" * (25 + len(df_name)))
    return duplicate_count

def check_referential_integrity(
    parent_df: DataFrame, parent_key: str,
    child_df: DataFrame, child_key: str,
    parent_df_name: str, child_df_name: str
):
    print(f"\n--- Referential Integrity Check: {child_df_name}.{child_key} against {parent_df_name}.{parent_key} ---")

    # Get unique keys from parent and child
    parent_keys = parent_df.select(parent_key).distinct()
    child_keys = child_df.select(child_key).distinct()

    # Find keys in child_df that are NOT in parent_df
    orphan_keys = child_keys.exceptAll(parent_keys)
    orphan_count = orphan_keys.count()

    if orphan_count > 0:
        print(f"Found {orphan_count} orphan keys in '{child_df_name}.{child_key}' that do not exist in '{parent_df_name}.{parent_key}'.")
        print("Sample orphan keys:")
        orphan_keys.show(5, truncate=False)
    else:
        print(f"All '{child_df_name}.{child_key}' values have a matching '{parent_df_name}.{parent_key}'. Referential integrity maintained.")
    print("-" * (50 + len(parent_df_name) + len(child_df_name) + len(parent_key) + len(child_key)))
    return orphan_count

def detect_outliers_zscore(df: DataFrame, df_name: str, numerical_cols: list):
    print(f"\n--- Outlier Detection (Z-score) for {df_name} ---")
    outliers_found = False
    outlier_dfs = {}

    for col_name in numerical_cols:
        # Calculate mean and standard deviation
        stats = df.agg(
            F.mean(F.col(col_name)).alias("mean"),
            F.stddev(F.col(col_name)).alias("stddev")
        ).collect()[0]

        col_mean = stats["mean"]
        col_stddev = stats["stddev"]

        if col_stddev is None or col_stddev == 0:
            print(f"Column '{col_name}': Cannot calculate Z-score (standard deviation is zero or null). Skipping.")
            continue

        # Calculate Z-score
        df_with_zscore = df.withColumn(
            f"{col_name}_zscore",
            (F.col(col_name) - col_mean) / col_stddev
        )

        # Filter for outliers
        outliers = df_with_zscore.filter(
            (F.abs(F.col(f"{col_name}_zscore")) > OUTLIER_THRESHOLD_Z_SCORE)
        )

        num_outliers = outliers.count()
        if num_outliers > 0:
            print(f"Column '{col_name}': {num_outliers} potential outliers detected (Z-score > {OUTLIER_THRESHOLD_Z_SCORE}).")
            outliers.select(col_name, f"{col_name}_zscore").show(5)
            outliers_found = True
            outlier_dfs[col_name] = outliers
        else:
            print(f"Column '{col_name}': No outliers detected (Z-score > {OUTLIER_THRESHOLD_Z_SCORE}).")

    if not outliers_found:
        print("No outliers detected in any specified numerical column.")
    print("-" * (25 + len(df_name)))
    return outlier_dfs

def check_business_logic_kpis(kpis_df: DataFrame, df_name: str):
    """
    Performs business logic checks on the calculated KPIs.
    - TotalCASA == TotalCurrentDeposits + TotalSavingsDeposits
    - TotalAggregateDeposits == TotalCASA + TotalTermDeposits
    - TotalAggregateCredit == TotalRetailCredit + TotalAgricultureCredit + TotalBusinessCredit
    """
    print(f"\n--- Business Logic Checks for {df_name} ---")
    logic_errors_found = False

    # Check 1: TotalCASA == TotalCurrentDeposits + TotalSavingsDeposits
    incorrect_casa = kpis_df.filter(
        F.col("TotalCASA") != (F.col("TotalCurrentDeposits") + F.col("TotalSavingsDeposits"))
    )
    if incorrect_casa.count() > 0:
        print("ERROR: TotalCASA does not equal (TotalCurrentDeposits + TotalSavingsDeposits) for some records.")
        incorrect_casa.select("Branches", "Year", "Quarter", "TotalCurrentDeposits", "TotalSavingsDeposits", "TotalCASA").show(5, truncate=False)
        logic_errors_found = True
    else:
        print("PASSED: TotalCASA logic check.")

    # Check 2: TotalAggregateDeposits == TotalCASA + TotalTermDeposits
    incorrect_agg_dep = kpis_df.filter(
        F.col("TotalAggregateDeposits") != (F.col("TotalCASA") + F.col("TotalTermDeposits"))
    )
    if incorrect_agg_dep.count() > 0:
        print("ERROR: TotalAggregateDeposits does not equal (TotalCASA + TotalTermDeposits) for some records.")
        incorrect_agg_dep.select("Branches", "Year", "Quarter", "TotalCASA", "TotalTermDeposits", "TotalAggregateDeposits").show(5, truncate=False)
        logic_errors_found = True
    else:
        print("PASSED: TotalAggregateDeposits logic check.")

    # Check 3: TotalAggregateCredit == TotalRetailCredit + TotalAgricultureCredit + TotalBusinessCredit
    incorrect_agg_credit = kpis_df.filter(
        F.col("TotalAggregateCredit") != (F.col("TotalRetailCredit") + F.col("TotalAgricultureCredit") + F.col("TotalBusinessCredit"))
    )
    if incorrect_agg_credit.count() > 0:
        print("ERROR: TotalAggregateCredit does not equal (TotalRetailCredit + TotalAgricultureCredit + TotalBusinessCredit) for some records.")
        incorrect_agg_credit.select("Branches", "Year", "Quarter", "TotalRetailCredit", "TotalAgricultureCredit", "TotalBusinessCredit", "TotalAggregateCredit").show(5, truncate=False)
        logic_errors_found = True
    else:
        print("PASSED: TotalAggregateCredit logic check.")

    if not logic_errors_found:
        print("All business logic checks passed successfully.")
    print("-" * (25 + len(df_name)))
    return not logic_errors_found # Returns True if all checks passed, False otherwise