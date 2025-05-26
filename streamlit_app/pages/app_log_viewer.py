# streamlit: Log Explorer
import streamlit as st
import pandas as pd
import re
from datetime import datetime

LOG_PATH = "src/logs/app.log"

st.set_page_config(layout="wide", page_title="Log Explorer")
st.title("Log Explorer")
st.write("*Dash to check batch processing logs*")

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

df = load_log(LOG_PATH)

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