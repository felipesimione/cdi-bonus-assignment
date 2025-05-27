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

st.subheader("Total Bonus Paid Per Day (with 14-day Moving Average)")
try:
    df_payouts = run_query("""
        SELECT payout_date, SUM(calculated_amount) as total_payout
        FROM daily_bonus_payouts
        GROUP BY payout_date
        ORDER BY payout_date
    """)
    df_payouts['payout_date'] = pd.to_datetime(df_payouts['payout_date'])
    df_payouts = df_payouts.sort_values('payout_date')
    df_payouts['14d_avg'] = df_payouts['total_payout'].rolling(window=14, min_periods=1).mean()
    df_payouts['%_vs_14d_avg'] = 100 * (df_payouts['total_payout'] - df_payouts['14d_avg']) / df_payouts['14d_avg']

    st.line_chart(
        df_payouts.set_index('payout_date')[['total_payout', '14d_avg']],
        use_container_width=True
    )

    st.dataframe(
        df_payouts[['payout_date', 'total_payout', '14d_avg', '%_vs_14d_avg']].tail(14).sort_values('payout_date', ascending=False),
        use_container_width=True
    )

    last_row = df_payouts.iloc[-1]
    if last_row['total_payout'] > last_row['14d_avg']:
        st.success(f"Last day total bonus paid: {last_row['total_payout']:.2f} (above 14-day average of {last_row['14d_avg']:.2f})")
    else:
        st.warning(f"Last day total bonus paid: {last_row['total_payout']:.2f} (below 14-day average of {last_row['14d_avg']:.2f})")

except Exception as e:
    st.error(f"Error loading payout data: {e}")

st.subheader("Eligible Users Per Day (with 14-day Moving Average)")
try:
    df_eligible_users = run_query("""
        SELECT payout_date, COUNT(DISTINCT user_id) as eligible_users_count
        FROM daily_bonus_payouts
        GROUP BY payout_date
        ORDER BY payout_date
    """)
    df_eligible_users['payout_date'] = pd.to_datetime(df_eligible_users['payout_date'])
    df_eligible_users = df_eligible_users.sort_values('payout_date')
 
    df_eligible_users['14d_avg'] = df_eligible_users['eligible_users_count'].rolling(window=14, min_periods=1).mean()
    df_eligible_users['%_vs_14d_avg'] = 100 * (df_eligible_users['eligible_users_count'] - df_eligible_users['14d_avg']) / df_eligible_users['14d_avg']

    st.line_chart(
        df_eligible_users.set_index('payout_date')[['eligible_users_count', '14d_avg']],
        use_container_width=True
    )

    st.dataframe(
        df_eligible_users[['payout_date', 'eligible_users_count', '14d_avg', '%_vs_14d_avg']].tail(14).sort_values('payout_date', ascending=False),
        use_container_width=True
    )

    last_row = df_eligible_users.iloc[-1]
    if last_row['eligible_users_count'] > last_row['14d_avg']:
        st.success(f"Last day eligible users: {int(last_row['eligible_users_count'])} (above 14-day average of {last_row['14d_avg']:.1f})")
    else:
        st.warning(f"Last day eligible users: {int(last_row['eligible_users_count'])} (below 14-day average of {last_row['14d_avg']:.1f})")

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