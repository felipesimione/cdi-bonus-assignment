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

st.subheader("Business Rule Check: Minimum Balance and 24hrs Rule")
try:
    df_dq_check = run_query(f"""
        SELECT
            dbp.payout_date,
            dbp.user_id,
            dbp.calculated_amount,
            wh.balance AS balance_at_start_of_day,
            wh.timestamp AS last_movement
        FROM daily_bonus_payouts dbp
        JOIN LATERAL (
            SELECT balance, timestamp
            FROM wallet_history
            WHERE user_id = dbp.user_id
            AND timestamp < dbp.payout_date
            ORDER BY timestamp DESC
            LIMIT 1
        ) wh ON TRUE
        WHERE dbp.calculated_amount > 0
        AND wh.balance <= 100
        AND dbp.payout_date - wh.timestamp < INTERVAL '24 hours'
        LIMIT 10;
    """)
    # To test the dashboard
    """
    If you need to test this dashboard, you can use the following SQL:
            SELECT
            dbp.payout_date,
            dbp.user_id,
            dbp.calculated_amount,
            wh.balance AS balance_at_start_of_day,
            wh.timestamp AS last_movement
        FROM daily_bonus_payouts dbp
        JOIN LATERAL (
            SELECT balance, timestamp
            FROM wallet_history
            WHERE user_id = dbp.user_id
            AND timestamp < dbp.payout_date
            ORDER BY timestamp DESC
            LIMIT 1
        ) wh ON TRUE
        WHERE dbp.calculated_amount > 0
        AND wh.balance <= 100
        -- Regra das 24 horas: o último movimento deve ser pelo menos 24h antes do payout_date
        AND dbp.payout_date - wh.timestamp < INTERVAL '24 hours'
        LIMIT 10;
    """
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