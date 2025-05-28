import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

from db import get_spark_jdbc_properties
from setup_data import generate_raw_cdc_data, insert_users, DEFAULT_NUM_LINES, DEFAULT_NUM_USERS, DEFAULT_OUTPUT_DIR, DEFAULT_OUTPUT_FILENAME
from src.extract.wallet_history import calculate_wallet_history
from src.transform.calculate_cdi_bonus import calculate_cdi_bonus_for_period
from src.extract.generate_daily_rates import insert_daily_rates_into_db
import logging

os.makedirs("src/logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("src/logs/app.log", mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
NUM_LINES = int(os.getenv("NUM_LINES", DEFAULT_NUM_LINES))
NUM_USERS = int(os.getenv("NUM_USERS", DEFAULT_NUM_USERS))
RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", os.path.join(DEFAULT_OUTPUT_DIR, DEFAULT_OUTPUT_FILENAME))

def create_spark_session():
    """
    Function to create a Spark session with the specified configurations.
    This function sets the application name, master URL, and JDBC properties for PostgreSQL.
    It also configures the legacy time parser policy to handle timestamp formats correctly.
    If the Spark session is created successfully, it prints a success message.
    If there is an error during the creation of the Spark session, it will raise an exception.
    Returns:
        SparkSession: The created Spark session.
    """

    logger.info("Creating Spark session...")
    spark = SparkSession.builder \
        .appName("CDIBonusCalculation") \
        .master(SPARK_MASTER_URL) \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.6.0.jar") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    logger.info("Spark session created successfully.")
    return spark

def step_generate_raw_data(RAW_DATA_PATH, NUM_LINES, NUM_USERS):
    """
    Function to generate raw CDC data.
    This function checks if the raw data file already exists.
    If the file does not exist, it calls the `generate_raw_cdc_data` function to create the data.
    If the file exists, it skips the generation step.
    Args:
        RAW_DATA_PATH (str): The path where the raw data file will be saved.
        NUM_LINES (int): The number of lines to generate in the raw data file.
        NUM_USERS (int): The number of users for whom the data will be generated.
    Returns:
        None
    """

    if not os.path.exists(RAW_DATA_PATH):
        logger.info(f"[MAIN] Raw data file not found ({RAW_DATA_PATH}). Starting generation...")
        generate_raw_cdc_data(NUM_LINES, NUM_USERS, os.path.dirname(RAW_DATA_PATH), os.path.basename(RAW_DATA_PATH))
        logger.info("[MAIN] Raw data generation completed.")
    else:
        logger.info(f"[MAIN] Raw data file found ({RAW_DATA_PATH}). Skipping generation.")

    raw_data_dir = os.path.dirname(RAW_DATA_PATH)
    raw_data_filename = os.path.basename(RAW_DATA_PATH)

    if not os.path.exists(RAW_DATA_PATH):
        logger.info(f"[MAIN] Raw data file not found ({RAW_DATA_PATH}). Starting generation...")
        generate_raw_cdc_data(NUM_LINES, NUM_USERS, raw_data_dir, raw_data_filename)
        logger.info("[MAIN] Raw data generation completed.")
    else:
        logger.info(f"[MAIN] Raw data file found ({RAW_DATA_PATH}). Skipping generation.")

def step_insert_users(NUM_USERS):
    """
    Function to insert users into the database.
    This function checks if the users already exist in the database and inserts them if not.
    It uses the `insert_users` function from the `setup_data` module.
    If the users already exist, it skips the insertion.
    Args:
        NUM_USERS (int): The number of users to insert into the database.
    Returns:
        None
    """

    logger.info("[MAIN] Starting user insertion into the database...")
    insert_users(NUM_USERS)
    logger.info("[MAIN] User insertion completed.")

def step_run_wallet_history_calculation(spark, RAW_DATA_PATH):
    """"
    Function to run the wallet history calculation.
    This function reads the raw CDC data from the specified path,
    applies the wallet history calculation, and writes the results to the database.
    Args:
        spark (SparkSession): The Spark session to use for processing.
        RAW_DATA_PATH (str): The path to the raw CDC data file.
    Returns:
        None
    """
    logger.info(f"[MAIN] Reading raw CDC file from: {RAW_DATA_PATH}")
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
        logger.info("[MAIN] Raw CDC file read successfully.")

        df_raw_cdc.show(5)
        df_raw_cdc.printSchema()

        logger.info("[MAIN] Calling the wallet history calculation function...")
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
        logger.info(f"[MAIN] Error reading the raw CDC file: {e}")
        spark.stop()
        exit(1)

if __name__ == "__main__":
    """"
    Main function to orchestrate the steps of the application.
    This function creates a Spark session, generates raw data, inserts users,
    runs the wallet history calculation, inserts daily rates into the database,
    and calculates the CDI bonus for the specified period.
    It handles the entire workflow of the application.
    Returns:
        None
    """
    logger.info("Starting the CDI bonus calculation application...")
    spark = create_spark_session()
    logger.info("Spark session created successfully.")
    
    logger.info(f"Using Spark master URL: {SPARK_MASTER_URL}")
    logger.info(f"Raw data path: {RAW_DATA_PATH}")
    logger.info(f"Number of lines to generate: {NUM_LINES}")
    logger.info(f"Number of users to generate: {NUM_USERS}")

    logger.info("Starting the data generation and processing steps...")
    logger.info("Generating raw data...")
    step_generate_raw_data(RAW_DATA_PATH, NUM_LINES, NUM_USERS)
    logger.info("Inserting users into the database...")
    step_insert_users(NUM_USERS)
    logger.info("Running wallet history calculation...")
    step_run_wallet_history_calculation(spark, RAW_DATA_PATH)
    logger.info("Inserting daily rates into the database...")
    insert_daily_rates_into_db()
    logger.info("Calculating CDI bonus for the period...")
    calculate_cdi_bonus_for_period(spark)
    logger.info("CDI bonus calculation completed successfully.")
    spark.stop()