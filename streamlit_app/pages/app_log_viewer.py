# streamlit: Log Explorer
import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import datetime

LOG_PATH = "src/logs/app.log"

st.set_page_config(layout="wide", page_title="Log Explorer")
st.title("Log Explorer")
st.write("*Dash to check batch processing logs*")

PIPELINE_STEPS = [
    "Generating raw data...",
    "Inserting users into the database...",
    "Running wallet history calculation...",
    "Inserting daily rates into the database...",
    "Calculating CDI bonus for the period..."
]

@st.cache_data
def load_log(path):
    log_lines = []
    log_pattern = re.compile(
        r'(?P<datetime>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) \[(?P<level>\w+)\] (?P<module>[\w\.]+): (?P<message>.*)'
    )
    with open(path, "r") as f:
        for line in f:
            match = log_pattern.match(line)
            if match:
                log_lines.append(match.groupdict())
    df = pd.DataFrame(log_lines)
    if not df.empty:
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S,%f')
    return df

def extract_step_timings(df, steps):
    step_rows = []
    for step in steps:
        mask = df['message'].str.contains(step)
        step_df = df[mask].copy()
        if not step_df.empty:
            first_row = step_df.iloc[0]
            step_rows.append({
                "step": step,
                "start_time": first_row['datetime']
            })
    timings = []
    for i, row in enumerate(step_rows):
        start = row["start_time"]
        end = step_rows[i+1]["start_time"] if i+1 < len(step_rows) else df['datetime'].max()
        duration = (end - start).total_seconds()
        timings.append({
            "Step": row["step"],
            "Start": start,
            "End": end,
            "Duration (s)": duration
        })
    return pd.DataFrame(timings)

st.subheader("Pipeline Step Timings")

df = load_log(LOG_PATH)

if not df.empty:
    timings_df = extract_step_timings(df, PIPELINE_STEPS)
    if not timings_df.empty:
        st.dataframe(timings_df)
        st.bar_chart(timings_df.set_index("Step")["Duration (s)"])
    else:
        st.info("No pipeline step timings found in the logs.")
else:
    st.info("No log data to analyze pipeline timings.")

if df.empty:
    st.warning("No log data found.")
    st.stop()

st.sidebar.header("Filters")
levels = df['level'].unique().tolist()
selected_levels = st.sidebar.multiselect("Log Level", levels, default=levels)
date_min = df['datetime'].min()
date_max = df['datetime'].max()
date_range = st.sidebar.date_input("Date Range", [date_min.date(), date_max.date()])

search_text = st.sidebar.text_input("Search in message")

filtered = df[
    df['level'].isin(selected_levels) &
    (df['datetime'].dt.date >= date_range[0]) &
    (df['datetime'].dt.date <= date_range[1])
]
if search_text:
    filtered = filtered[filtered['message'].str.contains(search_text, case=False, na=False)]

st.write(f"Showing {len(filtered)} log entries")

st.dataframe(filtered.sort_values("datetime", ascending=False), use_container_width=True)

st.subheader("Log Level Count")
st.bar_chart(filtered['level'].value_counts())

st.subheader("Log Timeline")
st.line_chart(filtered.groupby(filtered['datetime'].dt.floor('T')).size())