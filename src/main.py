import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

from db import get_spark_jdbc_properties
from setup_data import generate_raw_cdc_data, insert_users, DEFAULT_NUM_LINES, DEFAULT_NUM_USERS, DEFAULT_OUTPUT_DIR, DEFAULT_OUTPUT_FILENAME
from transformation.wallet_history import calculate_wallet_history
from src.transformation.calculate_cdi_bonus import calculate_cdi_bonus_for_period
from src.transformation.generate_daily_rates import insert_daily_rates_into_db

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
NUM_LINES = int(os.getenv("NUM_LINES", DEFAULT_NUM_LINES))
NUM_USERS = int(os.getenv("NUM_USERS", DEFAULT_NUM_USERS))
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", os.path.join(DEFAULT_OUTPUT_DIR, DEFAULT_OUTPUT_FILENAME))

def create_spark_session():
    print("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("CDIBonusCalculation") \
        .master(SPARK_MASTER_URL) \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    print("Spark session created successfully.")
    return spark

def step_generate_raw_data(RAW_DATA_PATH, NUM_LINES, NUM_USERS):
    if not os.path.exists(RAW_DATA_PATH):
        print(f"[MAIN] Raw data file not found ({RAW_DATA_PATH}). Starting generation...")
        generate_raw_cdc_data(NUM_LINES, NUM_USERS, os.path.dirname(RAW_DATA_PATH), os.path.basename(RAW_DATA_PATH))
        print("[MAIN] Raw data generation completed.")
    else:
        print(f"[MAIN] Raw data file found ({RAW_DATA_PATH}). Skipping generation.")

    raw_data_dir = os.path.dirname(RAW_DATA_PATH)
    raw_data_filename = os.path.basename(RAW_DATA_PATH)

    if not os.path.exists(RAW_DATA_PATH):
        print(f"[MAIN] Raw data file not found ({RAW_DATA_PATH}). Starting generation...")
        generate_raw_cdc_data(NUM_LINES, NUM_USERS, raw_data_dir, raw_data_filename)
        print("[MAIN] Raw data generation completed.")
    else:
        print(f"[MAIN] Raw data file found ({RAW_DATA_PATH}). Skipping generation.")


def step_insert_users(NUM_USERS):
    print("[MAIN] Starting user insertion into the database...")
    insert_users(NUM_USERS)
    print("[MAIN] User insertion completed.")


def step_run_wallet_history_calculation(spark, RAW_DATA_PATH):
    print(f"[MAIN] Reading raw CDC file from: {RAW_DATA_PATH}")
    raw_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("amount_change", FloatType(), True)
    ])

    try:
        df_raw_cdc = spark.read.csv(
            RAW_DATA_PATH,
            header=True,
            schema=raw_schema
        )
        print("[MAIN] Raw CDC file read successfully.")

        df_raw_cdc.show(5)
        df_raw_cdc.printSchema()

        print("[MAIN] Calling the wallet history calculation function...")
        df_wallet_history = calculate_wallet_history(df_raw_cdc)

        DB_TABLE_WALLET_HISTORY = "wallet_history"
        jdbc_url, jdbc_properties = get_spark_jdbc_properties()

        df_wallet_history.write.jdbc(
            url=jdbc_url,
            table=DB_TABLE_WALLET_HISTORY,
            mode="overwrite", 
            properties=jdbc_properties
        )

    except Exception as e:
        print(f"[MAIN] Error reading the raw CDC file: {e}")
        spark.stop()
        exit(1)

if __name__ == "__main__":
    spark = create_spark_session()
    
    step_generate_raw_data(RAW_DATA_PATH, NUM_LINES, NUM_USERS)
    step_insert_users(NUM_USERS)
    step_run_wallet_history_calculation(spark, RAW_DATA_PATH)
    insert_daily_rates_into_db()
    calculate_cdi_bonus_for_period(spark)
    spark.stop()