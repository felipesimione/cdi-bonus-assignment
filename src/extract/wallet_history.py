from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import sum, col, to_timestamp, lit, coalesce, monotonically_increasing_id
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def calculate_wallet_history(df_raw_cdc: DataFrame) -> DataFrame:
    """
    Calculates the wallet balance history table from raw CDC data.

    Args:
        df_raw_cdc (DataFrame): Spark DataFrame containing the raw CDC data
                                (user_id, timestamp, amount_change).
                                The 'timestamp' column is expected as StringType
                                in the format "yyyy-MM-dd'T'HH:mm:ss.SSSSSS".

    Returns:
        DataFrame: Spark DataFrame with the wallet balance history,
                   including the 'balance' column.
    """
    logger.info("Starting to build the portfolio history table...")

    TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

    df_raw_cdc_processed = df_raw_cdc.withColumn(
        "timestamp",
        coalesce(to_timestamp(col("timestamp"), TIMESTAMP_FORMAT), lit(datetime.now()))
    )
    logger.info("Column 'timestamp' processed and NULLs handled.")

    df_raw_cdc_with_id = df_raw_cdc_processed.withColumn("row_id", monotonically_increasing_id())
    logger.info("Unique row ID added for tiebreaker.")

    window_spec = Window.partitionBy("user_id").orderBy("timestamp", "row_id")
    logger.info("Window specification set (partitionBy user_id, orderBy timestamp).")

    df_wallet_history = df_raw_cdc_with_id.withColumn(
        "balance",
        sum(col("amount_change")).over(window_spec)
    ).drop("row_id")

    logger.info("Cumulative balance calculated successfully.")

    logger.info("History table of the constructed portfolio.")
    return df_wallet_history
