from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType
import pytest

from src.extract.wallet_history import calculate_wallet_history

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

def test_wallet_history_basic_cumulative(spark):
    data = [
        {"user_id": 1, "timestamp": "2024-05-01T10:00:00", "amount_change": 100},
        {"user_id": 1, "timestamp": "2024-05-03T10:00:00", "amount_change": -20},
        {"user_id": 1, "timestamp": "2024-05-04T10:00:00", "amount_change": 30},
        {"user_id": 2, "timestamp": "2024-05-02T10:00:00", "amount_change": 50},
    ]
    df = spark.createDataFrame(data)
    result = calculate_wallet_history(df)
    out = result.orderBy("user_id", "timestamp").select("user_id", "timestamp", "amount_change", "balance").collect()

    balances1 = [row.balance for row in out if row.user_id == 1]
    balances2 = [row.balance for row in out if row.user_id == 2]
    assert balances1 == [100, 80, 110]
    assert balances2 == [50]

def test_wallet_history_handles_null_timestamp(spark):
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("amount_change", IntegerType(), True),
    ])
    data = [{"user_id": 1, "timestamp": None, "amount_change": 50}]
    df = spark.createDataFrame(data, schema=schema)
    result = calculate_wallet_history(df)
    rows = result.collect()
    assert rows
    assert rows[0].balance == 50