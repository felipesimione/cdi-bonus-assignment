from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, unix_timestamp, max, row_number, to_timestamp, expr
from pyspark.sql.window import Window
from datetime import date, timedelta
from src.db import get_spark_jdbc_properties, get_db_connection, get_min_max_dates_from_wallet_history

def calculate_cdi_bonus_for_day(spark: SparkSession, calculation_date: date, jdbc_url: str, jdbc_properties: dict):
    """
    Calculates and records the CDI bonus for a single day.

    Args:
        spark (SparkSession): The active Spark session.
        calculation_date (date): The date for which the bonus will be calculated.
        jdbc_url (str): The JDBC connection URL for the database.
        jdbc_properties (dict): JDBC properties dictionary (user, password, driver).
    """
    print(f"\n--- Processing bonus for date: {calculation_date} ---")

    print(f"Reading interest rates from daily_interest_rates table for {calculation_date}...")
    daily_interest_rates_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "daily_interest_rates") \
        .options(**jdbc_properties) \
        .load() \
        .filter(col("rate_date") == calculation_date)

    if daily_interest_rates_df.count() == 0:
        print(f"Error: No interest rate found for date {calculation_date}. Skipping this day.")
        return

    current_daily_rate = daily_interest_rates_df.select("daily_rate").collect()[0][0]
    print(f"Interest rate for {calculation_date}: {current_daily_rate:.8f}")

    print(f"Reading data from wallet_history table to derive balances and last movements...")

    start_of_calculation_day_ts = to_timestamp(lit(calculation_date.strftime("%Y-%m-%d") + " 00:00:00"), "yyyy-MM-dd HH:mm:ss")

    wallet_history_raw_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "wallet_history") \
        .options(**jdbc_properties) \
        .load() \
        .filter(col("timestamp") < start_of_calculation_day_ts)

    if wallet_history_raw_df.count() == 0:
        print(f"No wallet history found before {calculation_date}. No bonus will be calculated for this day.")
        return

    window_spec = Window.partitionBy("user_id").orderBy(col("timestamp").desc())

    wallet_snapshot_df = wallet_history_raw_df \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .select(
            col("user_id"),
            col("balance").alias("balance_at_start_of_day"),
            col("timestamp").alias("last_movement_timestamp")
        )

    users_with_rate_df = wallet_snapshot_df.withColumn("current_daily_rate", lit(current_daily_rate))

    threshold_timestamp = start_of_calculation_day_ts - expr("INTERVAL 1 DAY")

    eligible_users_df = users_with_rate_df.filter(
        (col("balance_at_start_of_day") > 100) &
        (unix_timestamp(col("last_movement_timestamp")) <= unix_timestamp(threshold_timestamp))
    )

    print(f"Users Eligible for Bonus in {calculation_date}: {eligible_users_df.count()}")

    calculated_bonus_df = eligible_users_df.withColumn(
        "calculated_amount",
        col("balance_at_start_of_day") * col("current_daily_rate")
    ).withColumn(
        "payout_date",
        lit(calculation_date)
    ).select(
        col("payout_date"),
        col("user_id"),
        col("calculated_amount").cast("numeric(18, 2)")
    )

    print(f"CDI Bonus Calculated for Payout in {calculation_date}: {calculated_bonus_df.count()} records.")

    print(f"Writing results to daily_bonus_payouts table for {calculation_date}...")
    try:
        with get_db_connection() as conn:
            cur = conn.cursor()
            delete_sql = "DELETE FROM daily_bonus_payouts WHERE payout_date = %s;"
            cur.execute(delete_sql, (calculation_date,))
            conn.commit()
            print(f"Existing records for {calculation_date} deleted from daily_bonus_payouts.")

        calculated_bonus_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "daily_bonus_payouts") \
            .options(**jdbc_properties) \
            .mode("append") \
            .save()
        print(f"Data successfully inserted into daily_bonus_payouts for {calculation_date}.")
    except Exception as e:
        print(f"Error writing data to daily_bonus_payouts for {calculation_date}: {e}")


def calculate_cdi_bonus_for_period(spark: SparkSession, start_date_override: date = None, end_date_override: date = None):
    """
    Orchestrates the calculation of the CDI bonus for a period, day by day.

    Args:
        spark (SparkSession): The active Spark session, already configured.
        start_date_override (date, optional): Start date of the calculation period.
                                              If None, it will be determined automatically.
        end_date_override (date, optional): End date of the calculation period.
                                            If None, it will be determined automatically.
    """
    jdbc_url, jdbc_properties = get_spark_jdbc_properties()

    try:
        min_overall_date = None
        max_overall_date = None

        if start_date_override and end_date_override:
            min_overall_date = start_date_override
            max_overall_date = end_date_override
            if min_overall_date > max_overall_date:
                raise ValueError("The start date cannot be after the end date.")
            print(f"Calculation period set by arguments: {min_overall_date} to {max_overall_date}")
        else:
            min_wallet_date, max_wallet_date = get_min_max_dates_from_wallet_history()

            if not min_wallet_date or not max_wallet_date:
                print("Could not determine the period from wallet_history. Aborting calculation.")
                return

            min_overall_date = min_wallet_date + timedelta(days=1)
            max_overall_date = date.today() - timedelta(days=1)

            if max_wallet_date < max_overall_date:
                max_overall_date = max_wallet_date

            print(f"Calculation period determined automatically: {min_overall_date} to {max_overall_date}")

        current_calculation_date = min_overall_date
        while current_calculation_date <= max_overall_date:
            calculate_cdi_bonus_for_day(spark, current_calculation_date, jdbc_url, jdbc_properties)
            current_calculation_date += timedelta(days=1)

        print("\nHistorical CDI bonus calculation completed for the entire period.")

    except Exception as e:
        print(f"General error in CDI bonus calculation: {e}")
        raise