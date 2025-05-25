import csv
import random
import datetime
import os
from db import get_db_connection

DEFAULT_NUM_LINES = 10000
DEFAULT_NUM_USERS = 100   
DEFAULT_OUTPUT_DIR = "data/raw_cdc_files"
DEFAULT_OUTPUT_FILENAME = "wallet_cdc_raw.csv"
MIN_AMOUNT_CHANGE = -1000.0
MAX_AMOUNT_CHANGE = 5000.0
TIME_RANGE_DAYS = 90

def random_timestamp(start_date, end_date):
    """
    Generates a random timestamp (datetime object) within a specified date range,
    at second precision.
    """
    time_between_dates = end_date - start_date
    total_seconds = int(time_between_dates.total_seconds())
    if total_seconds <= 0:
        return start_date.replace(microsecond=0)
        
    random_seconds_offset = random.randrange(total_seconds)
    return (start_date + datetime.timedelta(seconds=random_seconds_offset)).replace(microsecond=0)

def generate_raw_cdc_data(num_lines, num_users, output_dir, output_filename):
    """
    Generates raw Change Data Capture (CDC) data for user wallets.
    Ensures globally unique and chronologically ordered timestamps (at second precision) 
    in the output CSV.
    """
    print(f"Generating {num_lines} raw CDC data for {num_users} users...")

    os.makedirs(output_dir, exist_ok=True)
    output_filepath = os.path.join(output_dir, output_filename)

    end_date = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    start_date = (end_date - datetime.timedelta(days=TIME_RANGE_DAYS)).replace(microsecond=0)

    all_records_with_initial_timestamps = []
    user_ids_pool = list(range(1, num_users + 1))

    for _ in range(num_lines):
        user_id = random.choice(user_ids_pool)
        amount_change = random.uniform(MIN_AMOUNT_CHANGE, MAX_AMOUNT_CHANGE)
        timestamp = random_timestamp(start_date, end_date) 
        
        all_records_with_initial_timestamps.append({
            'user_id': user_id,
            'timestamp': timestamp,
            'amount_change': round(amount_change, 2)
        })

    print("Sorting all records by initial timestamp...")

    all_records_with_initial_timestamps.sort(key=lambda x: x['timestamp'])

    print("Ensuring unique timestamps (at second precision) for all records...")
   
    final_records = []
    previous_timestamp = None 
    
    for record in all_records_with_initial_timestamps:
        current_timestamp = record['timestamp']
        
        if previous_timestamp and current_timestamp <= previous_timestamp:
            current_timestamp = previous_timestamp + datetime.timedelta(seconds=1)
        
        record['timestamp'] = current_timestamp.strftime('%Y-%m-%dT%H:%M:%S')
        final_records.append(record)
        previous_timestamp = current_timestamp

    print(f"Writing data to the file {output_filepath}...")
    with open(output_filepath, 'w', newline='') as csvfile:
        fieldnames = ['user_id', 'timestamp', 'amount_change']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(final_records)

    print(f"File generation completed. Saved in: {output_filepath}")
    print(f"Total number of lines generated: {len(final_records)}")

def insert_users(num_users):
    """
    Inserts a specified number of users into the 'users' table in the database.
    Handles conflicts to avoid re-inserting existing users.
    """
    print(f"[SETUP] Starting to insert {num_users} users into the 'users' table...")

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                insert_sql = "INSERT INTO users (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING;"
                user_ids_to_insert = [(i,) for i in range(1, num_users + 1)]

                cur.executemany(insert_sql, user_ids_to_insert)

            conn.commit()
            print(f"[SETUP] User inserted or already exists: {num_users}")

    except Exception as e:
        print(f"[SETUP] Error during user insertion: {e}")
        raise

    print("[SETUP] Users inserted successfully.")

if __name__ == "__main__":
    num_lines = DEFAULT_NUM_LINES
    num_users = DEFAULT_NUM_USERS
    output_dir = DEFAULT_OUTPUT_DIR
    output_filename = DEFAULT_OUTPUT_FILENAME

    generate_raw_cdc_data(num_lines, num_users, output_dir, output_filename)