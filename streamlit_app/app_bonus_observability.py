# streamlit: CDI Bonus Observability
import streamlit as st
import pandas as pd
from datetime import date, timedelta
from src.db import get_db_connection

st.set_page_config(layout="wide", page_title="CDI Bonus Observability")
st.title("CDI Bonus Observability Dashboard")
st.write("*Dash to check some quality and business metrics for CDI Bonus Payouts*")

def run_query(query):
    with get_db_connection() as conn:
        return pd.read_sql(query, conn)

st.header("Business Metrics")

st.subheader("Total Bonus Paid Per Day")
try:
    df_payouts = run_query("""
        SELECT payout_date, SUM(calculated_amount) as total_payout
        FROM daily_bonus_payouts
        GROUP BY payout_date
        ORDER BY payout_date DESC
        LIMIT 30
    """)
    st.line_chart(df_payouts.set_index('payout_date'))
except Exception as e:
    st.error(f"Error loading payout data: {e}")

st.subheader("Eligible Users Per Day")
try:
    df_eligible_users = run_query("""
        SELECT payout_date, COUNT(DISTINCT user_id) as eligible_users_count
        FROM daily_bonus_payouts
        GROUP BY payout_date
        ORDER BY payout_date DESC
        LIMIT 30
    """)
    st.bar_chart(df_eligible_users.set_index('payout_date'))
except Exception as e:
    st.error(f"Error loading eligible users data: {e}")

st.header("Data Quality")

st.subheader("Duplicate Bonus Payments Check")
try:
    df_duplicates = run_query("""
        SELECT payout_date, user_id, COUNT(*) as payment_count
        FROM daily_bonus_payouts
        GROUP BY payout_date, user_id
        HAVING COUNT(*) > 1
        ORDER BY payout_date DESC
        LIMIT 10
    """)
    if not df_duplicates.empty:
        st.warning("🚨 Data Quality Alert: Duplicate bonus payments detected!")
        st.dataframe(df_duplicates)
    else:
        st.success("✅ Duplicate Bonus Payments Check: No duplicates found.")
except Exception as e:
    st.error(f"Error in data quality check (Duplicate Payments): {e}")

st.subheader("Business Rule Check: Minimum Balance")
try:
    df_dq_check = run_query(f"""
        SELECT
            dbp.payout_date,
            dbp.user_id,
            dbp.calculated_amount
        FROM daily_bonus_payouts dbp
        WHERE dbp.calculated_amount > 0
        AND dbp.user_id IN (
            SELECT user_id FROM wallet_history
            WHERE balance <= 100
            AND timestamp < '{date.today() - timedelta(days=1)}'
        )
        LIMIT 10
    """)
    if not df_dq_check.empty:
        st.warning("🚨 Data Quality Alert: Bonus calculated for users with balance below the limit!")
        st.dataframe(df_dq_check)
    else:
        st.success("✅ Minimum Balance Check: No anomalies found.")
except Exception as e:
    st.error(f"Error in data quality check (Minimum Balance): {e}")


st.sidebar.header("Controls")
if st.sidebar.button("Refresh Dashboard Data"):
    st.rerun()