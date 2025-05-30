from bcb import sgs
import decimal
from datetime import date, timedelta
from src.db import get_db_connection, get_min_max_dates_from_wallet_history
import logging

logger = logging.getLogger(__name__)

def fetch_cdi_daily_rates(start_date, end_date):
    """
    Fetches the CDI rate (Series 4389 from BCB) for the specified period
    and converts it to a daily rate.
    The CDI rate from BCB is annual. To convert to daily (base 365 days):
    daily_rate = (1 + annual_rate/100)^(1/365) - 1
    """

    logger.info(f"Fetching CDI rates from BCB from {start_date} to {end_date}...")
    try:
        cdi_series = sgs.get(4389, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
    except Exception as e:
        logger.error(f"Error fetching series 4389 from BCB: {e}")
        logger.error("Check your internet connection or if the BCB service is available.")
        return {}

    daily_rates = {}
    for index, row in cdi_series.iterrows():
        current_date = index.date()
        annual_cdi_rate = row.values[0]

        try:
            annual_cdi_rate = decimal.Decimal(str(annual_cdi_rate))
        except decimal.InvalidOperation:
            logger.warning(f"Could not convert annual CDI rate '{annual_cdi_rate}' for {current_date} to Decimal. Skipping.")
            continue

        daily_factor = (decimal.Decimal('1') + annual_cdi_rate / decimal.Decimal('100'))**(decimal.Decimal('1')/decimal.Decimal('365')) - decimal.Decimal('1')
        daily_rates[current_date] = daily_factor.quantize(decimal.Decimal('0.0000000001'), rounding=decimal.ROUND_HALF_UP)

    logger.info(f"CDI rates fetched: {len(daily_rates)} days.")
    return daily_rates

def insert_daily_rates_into_db():
    """
    Fetches the actual CDI rates and inserts them into the daily_interest_rates table.
    The period is determined by the min/max date from wallet_history.
    """
    min_wallet_date, max_wallet_date = get_min_max_dates_from_wallet_history()

    if not min_wallet_date or not max_wallet_date:
        logger.info("Could not determine the period from wallet_history. Aborting.")
        return

    start_date_for_cdi = min_wallet_date - timedelta(days=7)
    end_date_for_cdi = max_wallet_date + timedelta(days=1)

    if end_date_for_cdi > date.today():
        end_date_for_cdi = date.today()

    cdi_data = fetch_cdi_daily_rates(start_date_for_cdi, end_date_for_cdi)

    if not cdi_data:
        logger.info("No CDI rates were obtained from BCB. Check the period or your connection.")
        return

    with get_db_connection() as conn:
        cur = conn.cursor()
        
        try:
            current_date = start_date_for_cdi
            while current_date <= end_date_for_cdi:
                daily_rate = cdi_data.get(current_date)
                
                if daily_rate is not None:
                    insert_sql = """
                    INSERT INTO daily_interest_rates (rate_date, daily_rate)
                    VALUES (%s, %s)
                    ON CONFLICT (rate_date) DO UPDATE SET daily_rate = EXCLUDED.daily_rate;
                    """
                    cur.execute(insert_sql, (current_date, daily_rate))
                else:
                    logger.info(f"Warning: No CDI rate found from BCB for {current_date}. Skipping this day.")
                
                current_date += timedelta(days=1)

            conn.commit()
            logger.info("\nData for daily_interest_rates generated and inserted successfully!")
        finally:
            cur.close()