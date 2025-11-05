import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from spark_jobs.config import RAW_DATA_PATH, NUM_BRANCHES

def load_raw_data(spark: SparkSession) -> DataFrame:
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(RAW_DATA_PATH)
        print(f"Successfully loaded data from: {RAW_DATA_PATH}")
        print("Schema:")
        df.printSchema()
        print("First 5 rows:")
        df.show(5)
        return df

    except Exception as e:
        print(f"An error occurred during raw data loading: {e}")
        print(f"Make sure the CSV file is at: {RAW_DATA_PATH}")
        raise

def generate_branch_location_data(spark: SparkSession) -> DataFrame:
    indian_locations = [
        ("Delhi", "New Delhi", "North"), ("Uttar Pradesh", "Lucknow", "North"), ("Uttar Pradesh", "Ghaziabad", "North"),
        ("Punjab", "Amritsar", "North"), ("Haryana", "Gurgaon", "North"), ("Rajasthan", "Jaipur", "North"),
        ("Himachal Pradesh", "Shimla", "North"), ("Jammu and Kashmir", "Srinagar", "North"),
        ("Maharashtra", "Mumbai", "West"), ("Maharashtra", "Pune", "West"), ("Gujarat", "Ahmedabad", "West"),
        ("Gujarat", "Surat", "West"), ("Goa", "Panaji", "West"), ("Madhya Pradesh", "Indore", "West"),
        ("Madhya Pradesh", "Bhopal", "West"), ("Chhattisgarh", "Raipur", "West"),
        ("Karnataka", "Bengaluru", "South"), ("Karnataka", "Mysuru", "South"), ("Tamil Nadu", "Chennai", "South"),
        ("Tamil Nadu", "Coimbatore", "South"), ("Kerala", "Kochi", "South"), ("Andhra Pradesh", "Hyderabad", "South"),
        ("Telangana", "Vijayawada", "South"), ("Puducherry", "Puducherry", "South"),
        ("West Bengal", "Kolkata", "East"), ("West Bengal", "Howrah", "East"), ("Odisha", "Bhubaneswar", "East"),
        ("Bihar", "Patna", "East"), ("Jharkhand", "Ranchi", "East"), ("Assam", "Guwahati", "East"),
        ("Sikkim", "Gangtok", "East"), ("Tripura", "Agartala", "East"),
    ]

    branch_names = [f"BRANCH-{i+1}" for i in range(NUM_BRANCHES)]

    if NUM_BRANCHES > len(indian_locations):
        repeated_locations = (indian_locations * ((NUM_BRANCHES // len(indian_locations)) + 1))[:NUM_BRANCHES]
    else:
        repeated_locations = indian_locations[:NUM_BRANCHES]

    location_data = pd.DataFrame({
        'Branches': branch_names,
        'State': [loc[0] for loc in repeated_locations],
        'City': [loc[1] for loc in repeated_locations],
        'Region': [loc[2] for loc in repeated_locations]
    })

    branch_locations_df = spark.createDataFrame(location_data)
    print(f"Generated dummy location data for {NUM_BRANCHES} branches.")
    branch_locations_df.show(5)
    return branch_locations_df