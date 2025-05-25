import os
import psycopg2
import time
from contextlib import contextmanager
from urllib.parse import urlparse

MAX_DB_RETRIES = 20
DB_RETRY_DELAY = 3

@contextmanager
def get_db_connection():
    """
    Context manager to obtain a connection to the PostgreSQL database.
    Reads credentials from environment variables and implements retry logic.
    Ensures the connection is closed when exiting the 'with' block.
    """
    db_url = os.getenv("DATABASE_URL")
    db_user = os.getenv("DATABASE_USER")
    db_password = os.getenv("DATABASE_PASSWORD")

    if not db_url or not db_user or not db_password:
        raise ValueError("Environment variables DATABASE_URL, DATABASE_USER or DATABASE_PASSWORD not set.")

    conn = None
    db_params = {}

    try:
        parsed_url = urlparse(db_url)
        db_params = {
            'database': parsed_url.path[1:],
            'user': db_user,
            'password': db_password,
            'host': parsed_url.hostname,
            'port': parsed_url.port if parsed_url.port else 5432
        }
    except Exception as e:
        raise ValueError(f"Error parsing DATABASE_URL '{db_url}': {e}")

    print(f"[DB] Trying to connect to the database at {db_params.get('host')}:{db_params.get('port')}/{db_params.get('database')}...")

    for i in range(MAX_DB_RETRIES):
        try:
            conn = psycopg2.connect(**db_params)
            print("[DB] Database connection established.")
            break 
        except psycopg2.OperationalError as e:
            print(f"[DB] Database connection error (Attempt {i+1}/{MAX_DB_RETRIES}): {e}")
            if i < MAX_DB_RETRIES - 1:
                print(f"[DB] Waiting {DB_RETRY_DELAY} seconds before trying again...")
                time.sleep(DB_RETRY_DELAY)
            else:
                print("[DB] Maximum number of connection attempts exceeded.")
                raise
        except Exception as e:
            print(f"[DB] Unexpected error connecting to the database (Attempt {i+1}/{MAX_DB_RETRIES}): {e}")
            if i < MAX_DB_RETRIES - 1:
                print(f"[DB] Waiting {DB_RETRY_DELAY} seconds before trying again...")
                time.sleep(DB_RETRY_DELAY)
            else:
                print("[DB] Maximum number of connection attempts exceeded.")
                raise

    if conn is None:
        raise ConnectionError("Could not establish a connection to the database after several attempts.")

    try:
        yield conn
    finally:
        if conn:
            conn.close()
            print("[DB] Database connection closed.")

def get_spark_jdbc_properties():
    """
    Returns a dictionary of JDBC connection properties for use with PySpark.
    """
    db_user = os.getenv("DATABASE_USER")
    db_password = os.getenv("DATABASE_PASSWORD")
    db_url = os.getenv("SPARK_JDBC_URL")

    if not db_user or not db_password:
        raise ValueError("Environment variables DATABASE_USER or DATABASE_PASSWORD not set for Spark JDBC.")

    jdbc_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    return db_url, jdbc_properties

def get_min_max_dates_from_wallet_history():
    """
    Fetches the minimum and maximum date from the wallet_history table.
    Returns (min_date, max_date) as date objects.
    """
    min_date = None
    max_date = None
    with get_db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT MIN(timestamp::date), MAX(timestamp::date) FROM wallet_history;")
        result = cur.fetchone()
        if result and result[0] and result[1]:
            min_date = result[0]
            max_date = result[1]
            print(f"[DB] Min/max dates found in wallet_history: {min_date} to {max_date}")
        else:
            print("[DB] No data found in wallet_history to determine the period.")
    return min_date, max_date