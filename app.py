import streamlit as st
import pandas as pd
import sqlite3

conn = sqlite3.connect("events.db")
df = pd.read_sql("SELECT * FROM events", conn)

st.title("Tracking Dashboard")
st.dataframe(df)

st.bar_chart(df["utm_source"].value_counts())
