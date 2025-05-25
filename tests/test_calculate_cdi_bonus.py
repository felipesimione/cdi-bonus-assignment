import pytest
from unittest.mock import patch, MagicMock, call
from datetime import date, datetime

from src.transformation.calculate_cdi_bonus import calculate_cdi_bonus_for_day, calculate_cdi_bonus_for_period

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType, DateType

@pytest.fixture(scope="session")
def spark_session():
    session = SparkSession.builder \
        .appName("PytestCDIBonusCalculation") \
        .master("local[2]") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    yield session
    session.stop()

def create_spark_df(spark, data, schema):
    return spark.createDataFrame(data, schema)

@patch('src.transformation.calculate_cdi_bonus.get_db_connection')
@patch('pyspark.sql.DataFrameWriter.save')
@patch('pyspark.sql.DataFrameReader.load')
def test_calculate_cdi_bonus_for_day_success_eligible_users(mock_load, mock_save, mock_get_db_connection, spark_session, mocker):
    """
    Tests successful bonus calculation for eligible users.
    """
    mock_print = mocker.patch('builtins.print')
    mock_jdbc_url = "jdbc:postgresql://testhost/testdb"
    mock_jdbc_properties = {"user": "testuser", "password": "testpassword"}
    calculation_date = date(2024, 5, 15)
    daily_rate_value = 0.00042

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db_connection.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    rates_schema = StructType([
        StructField("rate_date", DateType(), True),
        StructField("daily_rate", FloatType(), True)
    ])
    rates_data = [(calculation_date, daily_rate_value)]
    mock_rates_df = create_spark_df(spark_session, rates_data, rates_schema)

    wallet_history_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("balance", FloatType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    wallet_data = [
        (1, 2000.0, datetime(2024, 5, 10, 10, 0, 0)), # Elegible
        (2, 50.0, datetime(2024, 5, 10, 11, 0, 0)),   # No elegible (low balance)
        (3, 500.0, datetime(2024, 5, 14, 8, 0, 0)),  # No elegible (current moviment)
        (4, 300.0, datetime(2024, 5, 13, 23, 59, 59)) # Elegible
    ]
    mock_wallet_df = create_spark_df(spark_session, wallet_data, wallet_history_schema)

    mock_load.side_effect = [mock_rates_df, mock_wallet_df]

    calculate_cdi_bonus_for_day(spark_session, calculation_date, mock_jdbc_url, mock_jdbc_properties)

    assert mock_load.call_count == 2
    mock_get_db_connection.assert_called_once()
    mock_cursor.execute.assert_called_once_with("DELETE FROM daily_bonus_payouts WHERE payout_date = %s;", (calculation_date,))
    
    mock_print.assert_any_call(f"Users Eligible for Bonus in {calculation_date}: 2")
    mock_print.assert_any_call(f"CDI Bonus Calculated for Payout in {calculation_date}: 2 records.")
    mock_save.assert_called_once()


